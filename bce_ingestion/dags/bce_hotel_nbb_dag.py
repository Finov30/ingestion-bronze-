"""DAG Airflow — ciblage HÔTELLERIE + scraping des dépôts NBB (Jour 2).

Flux ::

    Silver (enterprise_silver)  ->  State DB entreprise (scrape_state)
                                ->  Airflow (mapping dynamique)
                                ->  CBSO/NBB  ->  Bronze HDFS (CSV >= 2021)

1. ``target_hotels`` : ouvre Mongo, garantit les index de la State DB entreprise,
   charge les hôtels ciblés en ``pending`` (``hotel.load_hotels_to_state``), puis
   renvoie un LOT de BCE encore à scraper (``state_db.pending_companies``,
   borné par ``config.BATCH_SIZE``) — léger et sérialisable via XCom.

2. ``scrape_one`` (dynamic task mapping, une instance par BCE) : rouvre Mongo +
   HDFS DANS la tâche, télécharge vers Bronze les CSV de dépôts NBB >= 2021 de
   l'entreprise, de façon idempotente (``scrape_nbb.scrape_company_filings``).

3. ``report`` : agrège done/skipped/error/filings_count et journalise les
   compteurs globaux de la State DB entreprise (``state_db.scrape_stats``).

Parse-time : AUCUNE connexion Mongo/HDFS au niveau module (toutes créées dans les
tâches) — le fichier s'importe sans MongoDB ni HDFS vivants.
"""
from __future__ import annotations

import logging
from datetime import datetime, timedelta

from airflow.decorators import dag, task

from bce_ingestion import config, hdfs_io, hotel, mongo, scrape_nbb, state_db

log = logging.getLogger(__name__)


@dag(
    dag_id="bce_hotel_nbb",
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_tasks=8,
    default_args={
        "owner": "data-eng",
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["bce", "hotel", "nbb"],
)
def bce_hotel_nbb():
    """Cible l'hôtellerie et scrape les dépôts financiers NBB vers Bronze."""

    @task
    def target_hotels() -> list:
        """Charge les hôtels en ``pending`` puis renvoie un lot de BCE à scraper."""
        db = mongo.get_db()
        state_db.ensure_scrape_indexes(db)
        loaded = hotel.load_hotels_to_state(db)
        pending = state_db.pending_companies(db, limit=config.BATCH_SIZE)
        log.info(
            "target_hotels : chargés=%s ; %d entreprise(s) à scraper (batch=%s)",
            loaded, len(pending), config.BATCH_SIZE,
        )
        return pending

    @task(map_index_template="{{ task.op_kwargs['bce'] }}")
    def scrape_one(bce: str) -> dict:
        """Scrape les dépôts CSV NBB (>= 2021) d'UNE entreprise vers Bronze."""
        db = mongo.get_db()
        hdfs = hdfs_io.get_hdfs()
        try:
            res = scrape_nbb.scrape_company_filings(db, hdfs, bce)
        except Exception as exc:  # noqa: BLE001 — n'échoue pas le mapping
            # L'entreprise reste 'in_progress' (reprenable) ; on renvoie un
            # comptage d'erreur pour que `report` reste robuste.
            log.warning("scrape_one bce=%s a échoué : %s", bce, exc)
            return {
                "bce": bce, "done": 0, "skipped": 0, "error": 1,
                "filings_count": 0, "company_error": str(exc),
            }
        res.setdefault("bce", bce)
        log.info("scrape_one bce=%s -> %s", bce, res)
        return res

    @task(trigger_rule="all_done")
    def report(results: list) -> dict:
        """Agrège les compteurs de scraping + stats State DB entreprise."""
        totals = {
            "companies": len(results or []),
            "done": 0, "skipped": 0, "error": 0, "filings_count": 0,
        }
        for r in results or []:
            if not isinstance(r, dict):
                continue
            totals["done"] += int(r.get("done", 0))
            totals["skipped"] += int(r.get("skipped", 0))
            totals["error"] += int(r.get("error", 0))
            totals["filings_count"] += int(r.get("filings_count", 0))

        db = mongo.get_db()
        scrape_state = state_db.scrape_stats(db)
        log.info(
            "RAPPORT hôtellerie/NBB — entreprises=%d done=%d skipped=%d error=%d filings=%d",
            totals["companies"], totals["done"], totals["skipped"],
            totals["error"], totals["filings_count"],
        )
        log.info("scrape_state stats: %s", scrape_state)
        return {"totals": totals, "scrape_state": scrape_state}

    # --- câblage : ciblage -> mapping dynamique -> rapport ------------------
    results = scrape_one.expand(bce=target_hotels())
    report(results)


# --- Instanciation au niveau module (requise par Airflow) ------------------
hotel_nbb_dag = bce_hotel_nbb()
