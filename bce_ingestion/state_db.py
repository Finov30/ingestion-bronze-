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
