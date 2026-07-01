"""DAG Airflow — Ingestion Bronze BCE (couche médaillon / Bronze).

Flux général
------------
    MongoDB (companies)  ->  State DB (file_state)  ->  Airflow  ->  Bronze HDFS

Ce DAG orchestre le *delta* d'ingestion à deux granularités :

1. `list_companies` (delta au niveau ENTREPRISE) :
   - ouvre une connexion Mongo (`mongo.get_db()`) et garantit les index de la
     State DB (`state_db.ensure_indexes`) ;
   - sélectionne un LOT (`BATCH_SIZE`) d'entreprises « à traiter » : celles qui
     ne sont pas encore marquées `ingest_status == 'done'`, OU dont le dernier
     passage (`last_ingest`) est plus ancien que `REFRESH_DAYS` jours ;
   - renvoie une liste de dicts `{'bce', 'forme', 'status'}` (léger, sérialisable
     via XCom).

2. `ingest_one` (delta au niveau FICHIER, via dynamic task mapping) :
   - une instance mappée par entreprise ;
   - rouvre db + HDFS À L'INTÉRIEUR de la tâche (jamais au parse-time) ;
   - délègue à `ingest.ingest_company()` qui, pour chaque fichier candidat des 3
     sources (NBB/CBSO, notaire.be, eJustice), consulte la State DB
     (`is_done`) et NE télécharge/écrit dans le Bronze que le delta.

3. `report` : agrège les dicts de résultats (done / skipped / error) et journalise
   en plus les compteurs globaux de la State DB (`state_db.stats`).

Important — parse-time
----------------------
AUCUNE connexion Mongo/HDFS n'est ouverte au niveau module : tout se fait dans
les tâches, de sorte que `airflow dags list` puisse importer ce fichier sans
MongoDB ni HDFS vivants.

Un second DAG (`bce_seed_companies`, manuel) permet de peupler la collection
`companies` depuis le KBO Open Data via `seed_companies.seed()`.
"""
from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone

from airflow.decorators import dag, task

from bce_ingestion import config, hdfs_io, ingest, mongo, seed_companies, state_db

log = logging.getLogger(__name__)

# Rafraîchit une entreprise déjà « done » après ce nombre de jours (nouvelle
# publication possible : nouveau dépôt NBB, nouvel acte, etc.).
REFRESH_DAYS = 30


# ===========================================================================
# DAG principal : ingestion Bronze
# ===========================================================================
@dag(
    dag_id="bce_bronze_ingestion",
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_tasks=8,
    default_args={
        "owner": "data-eng",
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["bce", "bronze"],
)
def bce_bronze_ingestion():
    """Pipeline d'ingestion Bronze : sélection d'un lot, ingestion mappée, rapport."""

    @task
    def list_companies(batch_size: int = config.BATCH_SIZE) -> list[dict]:
        """Delta au niveau entreprise : renvoie un lot d'entreprises à traiter.

        Une entreprise est « à traiter » si elle n'a jamais été ingérée
        (`ingest_status != 'done'` ou champ absent) ou si son dernier passage
        remonte à plus de `REFRESH_DAYS` jours.
        """
        db = mongo.get_db()
        # Garantit l'existence des index d'idempotence dès le début du run.
        state_db.ensure_indexes(db)

        cutoff = datetime.now(timezone.utc) - timedelta(days=REFRESH_DAYS)
        query = {
            "$or": [
                {"ingest_status": {"$ne": "done"}},
                {"last_ingest": {"$exists": False}},
                {"last_ingest": {"$lt": cutoff}},
            ]
        }

        companies = mongo.get_companies(db)
        out: list[dict] = []
        for doc in companies.find(query).limit(int(batch_size)):
            bce = str(doc.get("_id"))
            out.append(
                {
                    "bce": bce,
                    # tolérant aux variantes de nommage du champ « forme juridique »
                    "forme": doc.get("juridical_form")
                    or doc.get("forme")
                    or doc.get("forme_juridique")
                    or "",
                    "status": doc.get("status") or "Active",
                }
            )
        log.info("list_companies: %d entreprise(s) sélectionnée(s) (batch=%s)", len(out), batch_size)
        return out

    @task(map_index_template="{{ task.op_kwargs['company']['bce'] }}")
    def ingest_one(company: dict) -> dict:
        """Ingère une entreprise (les 3 sources) ; renvoie ses compteurs."""
        db = mongo.get_db()
        hdfs = hdfs_io.get_hdfs()
        result = ingest.ingest_company(
            db,
            hdfs,
            company["bce"],
            company.get("forme", ""),
            company.get("status", "Active"),
        )
        log.info("ingest_one bce=%s -> %s", company["bce"], result)
        return result

    @task
    def report(results: list[dict]) -> dict:
        """Agrège les compteurs de toutes les entreprises + stats State DB."""
        totals = {"companies": len(results), "done": 0, "skipped": 0, "error": 0}
        for r in results or []:
            if not isinstance(r, dict):
                continue
            # ingest_company renvoie les compteurs agrégés sous la clé 'totals'
            # (repli sur le niveau racine par robustesse).
            counts = r.get("totals") if isinstance(r.get("totals"), dict) else r
            totals["done"] += int(counts.get("done", 0))
            totals["skipped"] += int(counts.get("skipped", 0))
            totals["error"] += int(counts.get("error", 0))

        db = mongo.get_db()
        db_stats = state_db.stats(db)
        log.info(
            "RAPPORT ingestion Bronze — entreprises=%d  done=%d  skipped=%d  error=%d",
            totals["companies"], totals["done"], totals["skipped"], totals["error"],
        )
        log.info("State DB stats (source x status): %s", db_stats)
        return {"totals": totals, "state_db": db_stats}

    # --- câblage : mapping dynamique puis rapport -------------------------
    results = ingest_one.expand(company=list_companies())
    report(results)


# ===========================================================================
# DAG secondaire : seed de la collection `companies` (manuel)
# ===========================================================================
@dag(
    dag_id="bce_seed_companies",
    schedule=None,            # déclenchement manuel uniquement
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args={"owner": "data-eng", "retries": 1},
    tags=["bce", "seed"],
)
def bce_seed_companies():
    """Peuple/rafraîchit la collection `companies` depuis le KBO Open Data."""

    @task
    def seed() -> dict:
        db = mongo.get_db()
        state_db.ensure_indexes(db)
        result = seed_companies.seed(db)
        log.info("seed_companies terminé: %s", result)
        return result

    seed()


# --- Instanciation au niveau module (requis par Airflow) -------------------
dag_obj = bce_bronze_ingestion()
seed_dag_obj = bce_seed_companies()
