"""DAG Airflow — RECALCUL ANNUEL de la couche GOLD (Jour 3).

Recalcul incrémental annuel de ``config.GOLD_COLLECTION`` (``hotel_gold``) : à
chaque exercice, on ne recompute QUE les entreprises pour lesquelles la NBB
expose de nouveaux dépôts (nouvelles années) par rapport à ce qui est déjà en
Bronze / dans la State DB fichier (``file_state``).

Flux ::

    scrape_state (status == 'done')            -> list_done_companies
      -> check_new_filings (CBSO/NBB today vs file_state)  [dynamic mapping]
      -> recompute_gold (re-scrape Bronze si besoin + gold.build_gold + upsert)
      -> report

Idempotent & incrémental : une entreprise sans nouveau dépôt est « skipped »
(sa couche Gold reste valable). Une entreprise avec de nouvelles années est
re-scrapée vers Bronze (``scrape_nbb.scrape_company_filings``) puis sa Gold est
reconstruite et upsertée (``gold.build_gold``).

Parse-time : AUCUNE connexion Mongo / HDFS / réseau au niveau module — toutes
les connexions sont ouvertes DANS les tâches. Le module ``gold`` (livrable Jour
3 construisant/upsertant ``hotel_gold``) est importé PARESSEUSEMENT dans la tâche
qui l'utilise, de sorte que ``DagBag`` importe ce fichier même si ``gold`` n'est
pas encore présent (parsing du DAG découplé de la logique métier Gold).
"""
from __future__ import annotations

import logging
from datetime import datetime, timedelta

from airflow.decorators import dag, task

from bce_ingestion import config, hdfs_io, mongo, scrape_nbb, state_db
from bce_ingestion.sources import consult_nbb

log = logging.getLogger(__name__)

# Nom de la collection Gold (livrable Jour 3 : config.GOLD_COLLECTION). On tolère
# son absence à l'import pour ne pas coupler le parsing du DAG à ce réglage.
GOLD_COLLECTION = getattr(config, "GOLD_COLLECTION", "hotel_gold")


# ---------------------------------------------------------------------------
# Helpers (sans connexion : reçoivent `db` / des données déjà chargées)
# ---------------------------------------------------------------------------
def _done_csv_years(db, bce: str) -> set:
    """Années (int) de dépôts CSV NBB déjà ingérés en Bronze pour ``bce``.

    Source : State DB fichier (``config.STATE_COLLECTION`` = ``file_state``),
    documents ``(source='nbb', kind='csv', status='done')``. Sert de référence
    « ce qui est déjà en Bronze » pour la détection de delta annuel.
    """
    coll = db[config.STATE_COLLECTION]
    years: set = set()
    for doc in coll.find(
        {"source": "nbb", "bce": bce, "kind": "csv", "status": "done"},
        {"year": 1},
    ):
        try:
            years.add(int(doc.get("year")))
        except (TypeError, ValueError):
            continue
    return years


def _deposit_year(deposit: dict):
    """Année de clôture d'un dépôt (``periodEndDateYear``), en int si possible."""
    try:
        return int(deposit.get("periodEndDateYear"))
    except (TypeError, ValueError):
        return None


# ---------------------------------------------------------------------------
# DAG
# ---------------------------------------------------------------------------
@dag(
    dag_id="bce_gold_recalc",
    schedule="@yearly",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_tasks=8,
    default_args={
        "owner": "data-eng",
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["bce", "gold", "recalc", "annual"],
)
def bce_gold_recalc():
    """Recalcul annuel incrémental de la couche Gold (hotel_gold)."""

    @task
    def list_done_companies() -> list:
        """BCE dont le scraping est terminé (``scrape_state.status == 'done'``).

        Ce sont les seules entreprises candidates au recalcul Gold : leur Bronze
        NBB est complète à date. Renvoie une liste de BCE (sérialisable XCom).
        """
        db = mongo.get_db()
        coll = db[config.SCRAPE_STATE_COLLECTION]
        bces = [doc["_id"] for doc in coll.find({"status": "done"}, {"_id": 1})]
        log.info("list_done_companies : %d entreprise(s) 'done'", len(bces))
        return bces

    @task(map_index_template="{{ task.op_kwargs['bce'] }}")
    def check_new_filings(bce: str) -> dict:
        """Détecte les NOUVELLES années de dépôts pour ``bce`` (delta annuel).

        Compare les dépôts exposés AUJOURD'HUI par la CBSO/NBB
        (``consult_nbb.get_all_deposits``, filtrés ``>= config.YEAR_CSV_MIN``) à
        ce qui est déjà en Bronze (``file_state``, statut ``done``). Renvoie
        ``{'bce': bce, 'new_years': [...]}`` (années triées, sans doublon).

        Résilient : toute erreur réseau/listing -> ``new_years == []`` (aucun
        recalcul déclenché ; l'entreprise sera réévaluée au prochain run annuel).
        """
        db = mongo.get_db()
        known = _done_csv_years(db, bce)

        try:
            session = consult_nbb.make_session(bce)
            deposits = consult_nbb.get_all_deposits(session, bce)
        except Exception as exc:  # noqa: BLE001 — pas de réseau -> pas de delta
            log.warning("check_new_filings bce=%s : listing NBB KO (%s)", bce, exc)
            return {"bce": bce, "new_years": []}

        exposed: set = set()
        for dep in deposits:
            year = _deposit_year(dep)
            if year is not None and year >= config.YEAR_CSV_MIN:
                exposed.add(year)

        new_years = sorted(exposed - known)
        log.info(
            "check_new_filings bce=%s : exposées=%s ; connues=%s ; nouvelles=%s",
            bce, sorted(exposed), sorted(known), new_years,
        )
        return {"bce": bce, "new_years": new_years}

    @task(map_index_template="{{ task.op_kwargs['item']['bce'] }}")
    def recompute_gold(item: dict) -> dict:
        """Recompute la Gold d'UNE entreprise SI de nouveaux dépôts existent.

        - Aucune nouvelle année -> ``skipped`` (la Gold existante reste valable).
        - Nouvelles années -> (1) re-scrape Bronze des CSV manquants
          (``scrape_nbb.scrape_company_filings``, idempotent), puis (2)
          reconstruction Gold depuis la Bronze et upsert dans ``hotel_gold``
          (``gold.build_gold``). Le module ``gold`` est importé ICI (paresseux)
          pour découpler le parsing du DAG de sa présence.
        """
        bce = item.get("bce")
        new_years = item.get("new_years") or []

        if not new_years:
            log.info("recompute_gold bce=%s : aucune nouvelle année -> skip", bce)
            return {"bce": bce, "status": "skipped", "new_years": []}

        db = mongo.get_db()
        hdfs = hdfs_io.get_hdfs()

        # (1) Rafraîchit la Bronze (télécharge les CSV manquants). Idempotent :
        # les dépôts déjà en 'done' sont sautés. Une erreur n'empêche pas la
        # tentative de reconstruction Gold sur ce qui EST présent.
        scrape_summary = None
        try:
            scrape_summary = scrape_nbb.scrape_company_filings(db, hdfs, bce)
            log.info("recompute_gold bce=%s : re-scrape -> %s", bce, scrape_summary)
        except Exception as exc:  # noqa: BLE001 — on tente quand même la Gold
            log.warning("recompute_gold bce=%s : re-scrape KO (%s)", bce, exc)

        # (2) Reconstruit + upsert la Gold pour CETTE entreprise (import paresseux
        # du livrable Jour 3 ``gold``).
        try:
            from bce_ingestion import gold  # noqa: WPS433 — import local volontaire

            # Lit les CSV du Bronze HDFS pour CETTE entreprise, recalcule et
            # upserte sa Gold. Renvoie un résumé {'companies','years':int}.
            gold_res = gold.build_gold_from_bronze(db, hdfs, bce)
            log.info(
                "recompute_gold bce=%s : Gold reconstruite/upsertée dans %s (%s)",
                bce, GOLD_COLLECTION, gold_res,
            )
            years_out = gold_res.get("years") if isinstance(gold_res, dict) else None
            return {
                "bce": bce,
                "status": "recomputed",
                "new_years": new_years,
                "years_in_gold": years_out,
                "scrape": scrape_summary,
            }
        except Exception as exc:  # noqa: BLE001 — reste robuste pour `report`
            log.warning("recompute_gold bce=%s : build_gold KO (%s)", bce, exc)
            return {
                "bce": bce,
                "status": "error",
                "new_years": new_years,
                "error": str(exc),
                "scrape": scrape_summary,
            }

    @task(trigger_rule="all_done")
    def report(results: list) -> dict:
        """Agrège le résultat du recalcul annuel (recomputed / skipped / error)."""
        totals = {
            "companies": len(results or []),
            "recomputed": 0,
            "skipped": 0,
            "error": 0,
            "new_years_total": 0,
        }
        recomputed_bces: list = []
        for r in results or []:
            if not isinstance(r, dict):
                continue
            status = r.get("status")
            if status == "recomputed":
                totals["recomputed"] += 1
                recomputed_bces.append(r.get("bce"))
            elif status == "skipped":
                totals["skipped"] += 1
            else:
                totals["error"] += 1
            totals["new_years_total"] += len(r.get("new_years") or [])

        log.info(
            "RAPPORT GOLD (recalc annuel) — entreprises=%d recomputed=%d "
            "skipped=%d error=%d nouvelles_années=%d ; collection=%s",
            totals["companies"], totals["recomputed"], totals["skipped"],
            totals["error"], totals["new_years_total"], GOLD_COLLECTION,
        )
        if recomputed_bces:
            log.info("Entreprises recalculées : %s", recomputed_bces)
        return {"totals": totals, "recomputed": recomputed_bces}

    # --- câblage : done -> delta annuel -> recalcul incrémental -> rapport ---
    deltas = check_new_filings.expand(bce=list_done_companies())
    results = recompute_gold.expand(item=deltas)
    report(results)


# --- Instanciation au niveau module (requise par Airflow) ------------------
gold_recalc_dag = bce_gold_recalc()
