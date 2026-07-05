"""Accès MongoDB pour le backend.

- La connexion pymongo est *paresseuse* (créée au premier appel) : importer ce
  module ne touche jamais le réseau, ce qui permet les tests avec mongomock.
- ``get_db`` est une dépendance FastAPI ; les tests l'écrasent via
  ``app.dependency_overrides[get_db] = lambda: mongomock_db`` pour injecter une
  base factice pré-remplie.
- Les noms de collections proviennent de ``bce_ingestion.config`` quand le
  package est importable (mêmes noms que Jour 2/3), sinon des valeurs par défaut
  ci-dessous (surchargeables par variables d'environnement).
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

# ---------------------------------------------------------------------------
# Rendre le package bce_ingestion importable (réutilise config + strapor).
# Le package vit à /mnt/c/Users/HDCC5629/Downloads/bce_ingestion ; on ajoute son
# dossier PARENT au sys.path. Surchargeable via BCE_PACKAGE_PARENT / PYTHONPATH.
# ---------------------------------------------------------------------------
_DEFAULT_PARENT = "/mnt/c/Users/HDCC5629/Downloads"
_PARENT = os.environ.get("BCE_PACKAGE_PARENT", _DEFAULT_PARENT)
if _PARENT and _PARENT not in sys.path and Path(_PARENT).is_dir():
    sys.path.insert(0, _PARENT)

# ---------------------------------------------------------------------------
# Configuration Mongo (env-first, avec repli sur bce_ingestion.config).
# ---------------------------------------------------------------------------
MONGO_URI = os.environ.get("BCE_MONGO_URI", "mongodb://localhost:27017")
MONGO_DB = os.environ.get("BCE_MONGO_DB", "bce")


def _cfg(name: str, default: str) -> str:
    """Nom de collection depuis bce_ingestion.config si dispo, sinon défaut."""
    try:
        from bce_ingestion import config as _c  # noqa: WPS433 (import local voulu)

        return getattr(_c, name, default)
    except Exception:  # pragma: no cover - le package peut être absent
        return default


SILVER_COLLECTION = _cfg("SILVER_COLLECTION", os.environ.get("BCE_SILVER_COLLECTION", "enterprise_silver"))
GOLD_COLLECTION = _cfg("GOLD_COLLECTION", os.environ.get("BCE_GOLD_COLLECTION", "hotel_gold"))
DIRIGEANTS_COLLECTION = os.environ.get("BCE_DIRIGEANTS_COLLECTION", "dirigeants")
STATUTES_COLLECTION = os.environ.get("BCE_STATUTES_COLLECTION", "statutes")

# ---------------------------------------------------------------------------
# Connexion paresseuse.
# ---------------------------------------------------------------------------
_client = None
_db = None


def _get_client():
    global _client
    if _client is None:
        from pymongo import MongoClient  # import local (discipline Jour 2)

        _client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
    return _client


def get_db():
    """Dépendance FastAPI : renvoie la base Mongo (pymongo Database).

    Écrasable dans les tests via ``app.dependency_overrides``.
    """
    global _db
    if _db is None:
        _db = _get_client()[MONGO_DB]
    return _db
