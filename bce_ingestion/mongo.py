"""Aide de connexion MongoDB (import paresseux de pymongo).

pymongo est importé *à l'intérieur* des fonctions : dans l'environnement local
de tests, la pile pyOpenSSL est cassée et un `import pymongo` au niveau module
ferait échouer l'import du package entier. Toute la logique métier accepte par
ailleurs un objet `db` injecté (pymongo ou mongomock) — voir state_db.py.
"""
from __future__ import annotations

from . import config


def get_client(uri: str | None = None):
    """Renvoie un pymongo.MongoClient (pymongo importé paresseusement)."""
    import pymongo
    return pymongo.MongoClient(uri or config.MONGO_URI)


def get_db(client=None):
    """Renvoie la base de données configurée (`config.MONGO_DB`)."""
    return (client or get_client())[config.MONGO_DB]


def get_companies(db):
    """Collection des entreprises (config.COMPANIES_COLLECTION)."""
    return db[config.COMPANIES_COLLECTION]


def get_state(db):
    """Collection d'état / idempotence (config.STATE_COLLECTION)."""
    return db[config.STATE_COLLECTION]
