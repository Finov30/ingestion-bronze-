"""Orchestration centrale de l'ingestion Bronze (delta detection + idempotence).

Ce module relie les trois sources publiques (NBB/CBSO, notaire.be, eJustice) à la
State DB (idempotence) et à la couche Bronze HDFS. Le cœur est ``_process_file``
qui garantit qu'un fichier déjà téléchargé n'est JAMAIS re-téléchargé
(delta detection), via deux garde-fous complémentaires :

  1. ``state_db.is_done(...)``  — le fichier a déjà été marqué 'done' en base ;
  2. ``hdfs.exists(dest_rel)``  — le fichier est déjà présent sur HDFS.

Les modules ``state_db`` et ``sources.*`` sont écrits en parallèle : on les
importe donc PARESSEUSEMENT (à l'intérieur des fonctions) afin que ``ingest`` soit
importable même si ces modules ne sont pas encore présents, et pour rester
cohérent avec la contrainte "pas d'import pymongo au niveau module".

Contrats consommés (voir MEMORY / signatures) ::

    state_db.is_done(db, source, bce, kind, ref) -> bool
    state_db.mark_pending(db, source, bce, kind, ref, **meta)
    state_db.mark_done(db, source, bce, kind, ref, hdfs_path, **meta)
    state_db.mark_error(db, source, bce, kind, ref, error, **meta)

    hdfs_io.HdfsIO.exists(rel) -> bool
    hdfs_io.HdfsIO.put_bytes(data: bytes, rel) -> str  (chemin absolu)

    sources.consult_nbb.make_session(bce)
    sources.consult_nbb.get_all_deposits(session, bce) -> list[dict]
    sources.consult_nbb.fetch_csv_text(session, dep_id) -> str
    sources.consult_nbb.fetch_pdf_bytes(session, deposit) -> bytes

    sources.strapor_notaire.get_statutes(bce) -> list[dict]
    sources.strapor_notaire.fetch_statute_pdf_bytes(bce, statute) -> bytes | None
    sources.strapor_notaire.needs_notaire_check(forme, status) -> bool

    sources.ejustice.list_publications(bce) -> list[{date, numac, type, lien}]
    sources.ejustice.fetch_pdf_bytes(url) -> bytes

Layout Bronze (chemins RELATIFS transmis à HdfsIO.put_bytes) ::

    nbb/pdf/{bce}/{year}_{reference}.pdf
    nbb/csv/{bce}/{year}_{reference}.csv
    notaire/pdf/{bce}/{deedDate}_{documentId}.pdf
    ejustice/pdf/{bce}/{numac}.pdf
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone

from . import config

log = logging.getLogger(__name__)

# Statuts possibles renvoyés par _process_file (== clés des dict de comptage).
_STATUSES = ("done", "skipped", "error")


def _empty_counts() -> dict:
    return {"done": 0, "skipped": 0, "error": 0}


def _bump(counts: dict, status: str) -> None:
    """Incrémente le compteur correspondant (défensif si status inconnu)."""
    counts[status] = counts.get(status, 0) + 1


def _to_int(value) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


# ---------------------------------------------------------------------------
# Cœur : traitement idempotent d'UN fichier
# ---------------------------------------------------------------------------
def _process_file(db, hdfs, source, bce, kind, ref, dest_rel, fetch_fn, **meta):
    """Télécharge et pousse UN fichier vers Bronze, de manière idempotente.

    Retourne 'skipped' | 'done' | 'error'.

    - 'skipped' : déjà fait (state_db.is_done) OU déjà présent sur HDFS
                  (delta detection — on ne re-télécharge jamais).
    - 'done'    : téléchargé puis écrit sur Bronze avec succès (state 'done').
    - 'error'   : le fetch/l'écriture a échoué (state 'error', erreur consignée) ;
                  n'interrompt jamais le traitement des autres fichiers.
    """
    from . import state_db

    # 1) Delta detection --------------------------------------------------
    try:
        already_done = state_db.is_done(db, source, bce, kind, ref)
    except Exception:
        # Un souci de lecture d'état ne doit pas empêcher une éventuelle
        # re-écriture idempotente ; on retombe sur le contrôle HDFS.
        log.exception("[%s/%s/%s] is_done a échoué — on continue", source, bce, ref)
        already_done = False

    if already_done:
        return "skipped"
    try:
        if hdfs.exists(dest_rel):
            # Présent sur HDFS mais éventuellement pas encore 'done' en base :
            # on considère le fichier acquis (jamais de re-téléchargement).
            return "skipped"
    except Exception:
        log.exception("[%s] hdfs.exists(%s) a échoué — on tente le fetch", source, dest_rel)

    # 2) Téléchargement + écriture Bronze ---------------------------------
    state_db.mark_pending(db, source, bce, kind, ref, **meta)
    try:
        data = fetch_fn()
        if data is None:
            raise ValueError("aucune donnée renvoyée (fetch=None)")
        if isinstance(data, str):
            data = data.encode("utf-8")
        if not data:
            raise ValueError("contenu vide")
        path = hdfs.put_bytes(data, dest_rel)
        state_db.mark_done(db, source, bce, kind, ref, path, size=len(data), **meta)
        log.info("[%s] done %s -> %s (%d o)", source, ref, path, len(data))
        return "done"
    except Exception as e:  # 3) échec isolé — consigné, non fatal
        state_db.mark_error(db, source, bce, kind, ref, str(e), **meta)
        log.warning("[%s] error %s (%s): %s", source, bce, ref, e)
        return "error"


# ---------------------------------------------------------------------------
# Driver NBB / CBSO (comptes annuels : PDF + CSV)
# ---------------------------------------------------------------------------
def ingest_company_nbb(db, hdfs, bce, session=None) -> dict:
    """Ingestion des dépôts NBB/CBSO pour une entreprise.

    - PDF  : pour chaque dépôt d'année >= config.YEAR_PDF_MIN (ref = id du dépôt).
    - CSV  : uniquement si le dépôt n'est pas 'migration' ET année >= YEAR_CSV_MIN.
    """
    from .sources import consult_nbb

    counts = _empty_counts()
    if session is None:
        session = consult_nbb.make_session(bce)

    deposits = consult_nbb.get_all_deposits(session, bce)
    for dep in deposits:
        dep_id = dep.get("id")
        year = dep.get("periodEndDateYear")
        reference = dep.get("reference")
        ref = str(dep_id)
        year_int = _to_int(year)

        # --- PDF ---------------------------------------------------------
        if year_int is not None and year_int >= config.YEAR_PDF_MIN:
            dest = f"nbb/pdf/{bce}/{year}_{reference}.pdf"
            status = _process_file(
                db, hdfs, "nbb", bce, "pdf", ref, dest,
                lambda d=dep: consult_nbb.fetch_pdf_bytes(session, d),
                year=year, reference=reference,
            )
            _bump(counts, status)

        # --- CSV (indispo pour les dépôts migrés / legacy) ---------------
        if (not dep.get("migration")) and year_int is not None and year_int >= config.YEAR_CSV_MIN:
            dest = f"nbb/csv/{bce}/{year}_{reference}.csv"
            status = _process_file(
                db, hdfs, "nbb", bce, "csv", ref, dest,
                lambda i=dep_id: consult_nbb.fetch_csv_text(session, i),
                year=year, reference=reference,
            )
            _bump(counts, status)

    return counts


# ---------------------------------------------------------------------------
# Driver notaire.be (statuts)
# ---------------------------------------------------------------------------
def ingest_company_notaire(db, hdfs, bce, forme_juridique=None, status="Active") -> dict:
    """Ingestion des statuts notariés. Ignore les formes juridiques sans notaire
    et les entreprises non actives (via needs_notaire_check)."""
    from .sources import strapor_notaire

    counts = _empty_counts()
    if not strapor_notaire.needs_notaire_check(forme_juridique, status):
        log.info("[notaire] %s ignoré (forme=%s status=%s)", bce, forme_juridique, status)
        return counts

    statutes = strapor_notaire.get_statutes(bce)
    for st in statutes:
        doc_id = st.get("documentId")
        deed_raw = st.get("deedDate") or "unknown"
        deed_key = str(deed_raw).replace("-", "")  # sûr pour un nom de fichier
        ref = str(doc_id)
        dest = f"notaire/pdf/{bce}/{deed_key}_{doc_id}.pdf"
        res = _process_file(
            db, hdfs, "notaire", bce, "pdf", ref, dest,
            lambda s=st: strapor_notaire.fetch_statute_pdf_bytes(bce, s),
            deedDate=deed_raw, documentId=doc_id,
        )
        _bump(counts, res)

    return counts


# ---------------------------------------------------------------------------
# Driver eJustice (Moniteur belge — actes publiés)
# ---------------------------------------------------------------------------
def ingest_company_ejustice(db, hdfs, bce) -> dict:
    """Ingestion des publications eJustice (ref = numac)."""
    from .sources import ejustice

    counts = _empty_counts()
    publications = ejustice.list_publications(bce)
    for pub in publications:
        numac = pub.get("numac")
        url = pub.get("lien")
        # Certaines publications n'ont pas d'image PDF associée (``lien`` None) :
        # rien à télécharger — on saute (sinon 'error' perpétuel, jamais idempotent).
        if not url:
            _bump(counts, "skipped")
            continue
        ref = str(numac)
        dest = f"ejustice/pdf/{bce}/{numac}.pdf"
        res = _process_file(
            db, hdfs, "ejustice", bce, "pdf", ref, dest,
            lambda u=url: ejustice.fetch_pdf_bytes(u),
            numac=numac, type=pub.get("type"), date=pub.get("date"),
        )
        _bump(counts, res)

    return counts


# ---------------------------------------------------------------------------
# Orchestrateur par entreprise (agrège les 3 sources)
# ---------------------------------------------------------------------------
_DRIVERS = {
    "nbb": lambda db, hdfs, bce, forme, status: ingest_company_nbb(db, hdfs, bce),
    "notaire": lambda db, hdfs, bce, forme, status: ingest_company_notaire(db, hdfs, bce, forme, status),
    "ejustice": lambda db, hdfs, bce, forme, status: ingest_company_ejustice(db, hdfs, bce),
}


def ingest_company(db, hdfs, bce, forme_juridique=None, status="Active",
                   sources=("nbb", "notaire", "ejustice")) -> dict:
    """Ingère une entreprise sur toutes les sources demandées.

    Chaque source est isolée : une source qui plante (réseau, cookies, etc.)
    n'interrompt jamais les autres — son échec est consigné dans le résultat.
    Met à jour le document company (last_ingest / ingest_status / ingest_counts).
    """
    result = {"bce": bce}

    for src in sources:
        driver = _DRIVERS.get(src)
        if driver is None:
            log.warning("source inconnue ignorée : %r", src)
            result[src] = {**_empty_counts(), "source_error": f"unknown source {src!r}"}
            continue
        try:
            result[src] = driver(db, hdfs, bce, forme_juridique, status)
        except Exception as e:
            log.exception("[%s] driver %s a échoué pour %s", src, src, bce)
            # Un dossier de comptage minimal signalant l'échec de la source.
            result[src] = {**_empty_counts(), "error": 1, "source_error": str(e)}

    # --- Agrégat + mise à jour du document company -----------------------
    agg = _empty_counts()
    for src in sources:
        c = result.get(src)
        if isinstance(c, dict):
            for k in _STATUSES:
                agg[k] += c.get(k, 0)
    result["totals"] = agg

    ingest_status = "error" if agg["error"] else "done"
    result["ingest_status"] = ingest_status

    try:
        db[config.COMPANIES_COLLECTION].update_one(
            {"_id": bce},
            {"$set": {
                "last_ingest": datetime.now(timezone.utc),
                "ingest_status": ingest_status,
                "ingest_counts": agg,
            }},
        )
    except Exception:
        # La MàJ du document company ne doit jamais faire échouer l'ingestion.
        log.exception("[company] MàJ du document %s impossible", bce)

    return result
