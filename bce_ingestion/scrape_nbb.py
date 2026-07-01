"""Scraping des dépôts comptables NBB/CBSO vers la couche Bronze (Jour 2).

Pour une entreprise (numéro BCE à 10 chiffres avec zéro de tête), on récupère
l'historique des dépôts publiés au CBSO (``sources.consult_nbb``), on ne garde
que les exercices ``periodEndDateYear >= min_year`` (2021 par défaut, cf.
``config.YEAR_CSV_MIN``) et on télécharge le CSV de chaque dépôt vers HDFS Bronze
selon la disposition imposée par l'énoncé :

    /<bce>/nbb/<year>/<reference>.csv

L'idempotence repose sur DEUX niveaux de State DB (aucun import pymongo ici — le
``db`` est toujours injecté : pymongo sur le cluster, mongomock en test) :

- niveau FICHIER (``state_db`` per-file, collection ``file_state``) : un document
  par CSV, clé ``(source='nbb', bce, kind='csv', ref=reference)``. ``is_done``
  sert de détection de delta ; on saute tout fichier déjà en statut ``done``.
- niveau ENTREPRISE (``state_db`` company-level, collection ``scrape_state``) :
  un document ``_id == bce`` avec un statut ``pending -> in_progress -> done``.
  On passe l'entreprise en ``in_progress`` au début, puis en ``done`` (avec le
  nombre de dépôts ciblés) UNIQUEMENT si aucune erreur n'est survenue. En cas
  d'erreur (p. ex. un 429 en cours de route) l'entreprise reste ``in_progress``
  et RESTE donc « à reprendre » : au prochain run les CSV déjà téléchargés sont
  sautés (``skipped``) et seuls les manquants sont retentés.

Fonctions company-level de ``state_db`` (ajoutées en parallèle Jour 2) utilisées :
  set_company_status(db, bce, status, **meta)   -> upsert scrape_state (_id=bce)
  pending_companies(db, limit) -> list[str]     -> BCE encore à scraper
"""
from __future__ import annotations

from . import config
from . import state_db
from .hdfs_io import HdfsIO
from .sources import consult_nbb


SOURCE = "nbb"
KIND = "csv"


def _deposit_year(deposit: dict):
    """Année de clôture d'un dépôt (``periodEndDateYear``), en int si possible."""
    year = deposit.get("periodEndDateYear")
    try:
        return int(year)
    except (TypeError, ValueError):
        return None


def scrape_company_filings(
    db,
    hdfs: HdfsIO,
    bce: str,
    min_year: int = config.YEAR_CSV_MIN,
    session=None,
) -> dict:
    """Scrape les dépôts CSV NBB (>= ``min_year``) d'une entreprise vers Bronze.

    Renvoie ``{'done': n, 'skipped': n, 'error': n, 'filings_count': n}`` où
    ``filings_count`` est le nombre de dépôts retenus (année >= ``min_year``).

    Idempotent / reprenable : un CSV déjà en statut ``done`` (ou déjà présent sur
    HDFS) est ``skipped``. L'entreprise n'est marquée ``done`` que si tous les
    téléchargements ont réussi ; sinon elle reste ``in_progress`` (reprenable).
    """
    done = skipped = error = 0
    last_error: str | None = None

    # Entreprise « en cours » : verrou logique + reprise possible.
    state_db.set_company_status(db, bce, "in_progress")

    session = session or consult_nbb.make_session(bce)

    # Le listing lui-même peut échouer (réseau / 429 non résolu) : on laisse
    # l'entreprise en 'in_progress' (reprenable) et on remonte l'erreur.
    try:
        deposits = consult_nbb.get_all_deposits(session, bce)
    except Exception as exc:  # noqa: BLE001 — on veut toute erreur de listing
        state_db.set_company_status(db, bce, "in_progress", error=str(exc))
        raise

    # On ne garde que les exercices >= min_year (periodEndDateYear).
    kept = [d for d in deposits if (_deposit_year(d) or -1) >= min_year]
    filings_count = len(kept)

    for deposit in kept:
        ref = deposit["reference"]
        year = _deposit_year(deposit)
        dep_id = deposit["id"]
        rel = f"{bce}/nbb/{year}/{ref}.csv"

        # DELTA : déjà fait (State DB) ou déjà présent sur HDFS -> on saute.
        if state_db.is_done(db, SOURCE, bce, KIND, ref) or hdfs.exists(rel):
            skipped += 1
            continue

        try:
            text = consult_nbb.fetch_csv_text(session, dep_id)
            path = hdfs.put_bytes(text.encode("utf-8"), rel)
            state_db.mark_done(
                db, SOURCE, bce, KIND, ref, path, year=year, reference=ref
            )
            done += 1
        except Exception as exc:  # noqa: BLE001 — une erreur ne stoppe pas le lot
            last_error = str(exc)
            state_db.mark_error(
                db, SOURCE, bce, KIND, ref, last_error, year=year, reference=ref
            )
            error += 1

    # Fin de traitement : 'done' ssi aucune erreur ; sinon reprenable.
    if error == 0:
        state_db.set_company_status(
            db, bce, "done", filings_count=filings_count
        )
    else:
        state_db.set_company_status(
            db, bce, "in_progress", filings_count=filings_count, error=last_error
        )

    return {
        "done": done,
        "skipped": skipped,
        "error": error,
        "filings_count": filings_count,
    }


def scrape_pending_hotels(
    db,
    hdfs: HdfsIO,
    limit: int = config.BATCH_SIZE,
) -> dict:
    """Scrape les entreprises ciblées encore en attente (``scrape_state``).

    Parcourt ``state_db.pending_companies(db, limit)`` et applique
    ``scrape_company_filings`` à chacune. Une entreprise qui échoue (listing en
    erreur) n'interrompt pas le lot : elle est comptée dans ``companies_error``
    et reste reprenable. Renvoie un résumé agrégé.
    """
    summary = {
        "companies": 0,
        "companies_done": 0,
        "companies_error": 0,
        "done": 0,
        "skipped": 0,
        "error": 0,
        "filings_count": 0,
    }

    for bce in state_db.pending_companies(db, limit):
        summary["companies"] += 1
        try:
            res = scrape_company_filings(db, hdfs, bce)
        except Exception:  # noqa: BLE001 — l'entreprise reste 'in_progress'
            summary["companies_error"] += 1
            continue
        summary["done"] += res["done"]
        summary["skipped"] += res["skipped"]
        summary["error"] += res["error"]
        summary["filings_count"] += res["filings_count"]
        if res["error"] == 0:
            summary["companies_done"] += 1
        else:
            summary["companies_error"] += 1

    return summary
