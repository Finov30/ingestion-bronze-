"""State DB — idempotence & delta-detection pour l'ingestion Bronze.

Une seule collection (`config.STATE_COLLECTION`) contient un document par
fichier à ingérer, identifié de façon unique par la clé logique
`(source, bce, kind, ref)` :

- source : 'nbb' | 'notaire' | 'ejustice'
- bce    : numéro BCE à 10 chiffres AVEC zéro de tête (ex. '0203430576')
- kind   : 'csv' | 'pdf'
- ref    : identifiant unique du fichier au sein de (source, bce, kind)

Champs du document :
  source, bce, kind, ref,                (clé unique)
  year, reference,                       (métadonnées utiles au delta/tri)
  status  in {'pending','done','error'},
  hdfs_path,                             (renseigné à 'done')
  attempts,                              ($inc à chaque erreur)
  error,                                 (dernier message d'erreur)
  created_at, updated_at                 (datetime aware UTC)

Toutes les écritures passent par update_one(filter=key, ..., upsert=True) : re-
lancer un DAG ne crée jamais de doublon. `is_done()` est la primitive de
détection de delta (on saute les fichiers déjà en statut 'done').

Le `db` est TOUJOURS injecté (pymongo ou mongomock) — aucun import pymongo ici.
"""
from __future__ import annotations

from datetime import datetime, timezone

from . import config


# ---------------------------------------------------------------------------
# Helpers internes
# ---------------------------------------------------------------------------
def _now():
    return datetime.now(timezone.utc)


def _coll(db):
    return db[config.STATE_COLLECTION]


def _key(source: str, bce: str, kind: str, ref) -> dict:
    """Clé logique unique d'un fichier (ref normalisé en str)."""
    return {"source": source, "bce": bce, "kind": kind, "ref": str(ref)}


# ---------------------------------------------------------------------------
# Index
# ---------------------------------------------------------------------------
def ensure_indexes(db) -> None:
    """Crée l'index unique sur la clé logique + quelques index utiles."""
    coll = _coll(db)
    coll.create_index(
        [("source", 1), ("bce", 1), ("kind", 1), ("ref", 1)],
        unique=True,
        name="uq_source_bce_kind_ref",
    )
    # balayage par avancement (stats, reprise des erreurs)
    coll.create_index([("source", 1), ("status", 1)], name="ix_source_status")
    # accès par entreprise
    coll.create_index([("bce", 1)], name="ix_bce")


# ---------------------------------------------------------------------------
# Detection de delta
# ---------------------------------------------------------------------------
def is_done(db, source: str, bce: str, kind: str, ref) -> bool:
    """True ssi un document existe avec status == 'done' pour cette clé."""
    doc = _coll(db).find_one(_key(source, bce, kind, ref), {"status": 1})
    return bool(doc) and doc.get("status") == "done"


# ---------------------------------------------------------------------------
# Transitions d'état (toutes idempotentes / upsert)
# ---------------------------------------------------------------------------
def mark_pending(db, source: str, bce: str, kind: str, ref, **meta) -> None:
    """Upsert en statut 'pending'. Ne crée jamais de doublon (upsert sur la clé)."""
    key = _key(source, bce, kind, ref)
    now = _now()
    set_fields = {"status": "pending", "updated_at": now}
    set_fields.update(meta)
    _coll(db).update_one(
        key,
        {
            "$setOnInsert": {**key, "created_at": now, "attempts": 0},
            "$set": set_fields,
        },
        upsert=True,
    )


def mark_done(db, source: str, bce: str, kind: str, ref, hdfs_path: str, **meta) -> None:
    """Upsert en statut 'done' et enregistre le chemin HDFS final."""
    key = _key(source, bce, kind, ref)
    now = _now()
    set_fields = {"status": "done", "hdfs_path": hdfs_path, "updated_at": now}
    set_fields.update(meta)
    _coll(db).update_one(
        key,
        {
            "$setOnInsert": {**key, "created_at": now, "attempts": 0},
            "$set": set_fields,
        },
        upsert=True,
    )


def mark_error(db, source: str, bce: str, kind: str, ref, error, **meta) -> None:
    """Upsert en statut 'error', incrémente `attempts`, stocke le message."""
    key = _key(source, bce, kind, ref)
    now = _now()
    set_fields = {"status": "error", "error": str(error), "updated_at": now}
    set_fields.update(meta)
    _coll(db).update_one(
        key,
        {
            "$setOnInsert": {**key, "created_at": now},
            "$set": set_fields,
            "$inc": {"attempts": 1},
        },
        upsert=True,
    )


# ---------------------------------------------------------------------------
# Statistiques
# ---------------------------------------------------------------------------
def stats(db) -> dict:
    """Comptes groupés par (source, status), ex. {'nbb': {'done': 12, 'error': 1}}."""
    out: dict = {}
    pipeline = [
        {"$group": {"_id": {"source": "$source", "status": "$status"}, "n": {"$sum": 1}}}
    ]
    for row in _coll(db).aggregate(pipeline):
        source = row["_id"].get("source")
        status = row["_id"].get("status")
        out.setdefault(source, {})[status] = row["n"]
    return out


# ===========================================================================
# State DB au niveau ENTREPRISE (Jour 2) — collection config.SCRAPE_STATE_COLLECTION
# ---------------------------------------------------------------------------
# Ici la granularité n'est plus le fichier mais l'ENTREPRISE : un document par
# BCE (le `_id` EST le bce10), qui trace l'avancement du scraping des dépôts NBB
# pour cette entreprise. Statuts (par énoncé) :
#   'pending'      -> à traiter (chargée depuis le ciblage hôtellerie)
#   'in_progress'  -> scraping en cours (worker actif)
#   'done'         -> tous les dépôts de l'entreprise ont été ingérés
#
# Ces fonctions sont INDÉPENDANTES des primitives par-fichier ci-dessus : elles
# vivent dans une autre collection et n'en cassent aucune.
# ===========================================================================
def _scrape_coll(db):
    return db[config.SCRAPE_STATE_COLLECTION]


def ensure_scrape_indexes(db) -> None:
    """Crée l'index de balayage par statut (reprise/stats du scraping)."""
    _scrape_coll(db).create_index([("status", 1)], name="ix_scrape_status")


def set_company_status(db, bce: str, status: str, **fields) -> None:
    """Upsert l'état de scraping d'une entreprise (idempotent).

    Document : {_id: bce, status, updated_at, created_at, **fields}. `created_at`
    est posé uniquement à l'insertion ($setOnInsert) ; `**fields` (ex.
    filings_count) est écrit à chaque appel.
    """
    now = _now()
    set_fields = {"status": status, "updated_at": now}
    set_fields.update(fields)
    _scrape_coll(db).update_one(
        {"_id": bce},
        {
            "$setOnInsert": {"created_at": now},
            "$set": set_fields,
        },
        upsert=True,
    )


def get_company_status(db, bce: str) -> str | None:
    """Renvoie le statut de scraping d'une entreprise, ou None si inconnue."""
    doc = _scrape_coll(db).find_one({"_id": bce}, {"status": 1})
    return doc.get("status") if doc else None


def pending_companies(db, limit: int | None = None) -> list:
    """Liste des BCE à scraper : statut 'pending' OU 'in_progress' (repris).

    Un 'in_progress' correspond à un worker interrompu : on le remet dans la file
    de travail. `limit` borne le nombre de BCE renvoyés (batching des DAG).
    """
    cur = _scrape_coll(db).find(
        {"status": {"$in": ["pending", "in_progress"]}}, {"_id": 1}
    )
    if limit is not None:
        cur = cur.limit(limit)
    return [doc["_id"] for doc in cur]


def scrape_stats(db) -> dict:
    """Comptes par statut, ex. {'pending': 12, 'in_progress': 1, 'done': 187}."""
    out = {"pending": 0, "in_progress": 0, "done": 0}
    pipeline = [{"$group": {"_id": "$status", "n": {"$sum": 1}}}]
    for row in _scrape_coll(db).aggregate(pipeline):
        out[row["_id"]] = row["n"]
    return out
