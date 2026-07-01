"""Ciblage du secteur HÔTELLERIE (Jour 2).

À partir de la collection Bronze « riche » (``config.FINALE_COLLECTION``, ou la
couche Silver ``config.SILVER_COLLECTION``), on sélectionne les entreprises
d'hébergement touristique ACTIVES, puis on les charge dans la State DB au niveau
entreprise (``config.SCRAPE_STATE_COLLECTION``) en statut ``pending`` afin de
piloter, de façon idempotente, le scraping des dépôts financiers NBB.

Critères de ciblage (par énoncé) :
  - ``Status == 'AC'``                    (entreprise active)
  - ``TypeOfEnterprise == '2'``           (personne morale)
  - ``JuridicalForm`` NOT IN ``EXCLUDED_FORMS``  (exclut ASBL / formes publiques…)
  - une activité PRINCIPALE ('MAIN') dont le ``NaceCode`` est un code hôtellerie.

Le document `finale` expose des champs à la casse KBO (``Status``,
``TypeOfEnterprise``, ``JuridicalForm``) et un tableau ``activities`` d'objets
``{Classification, NaceCode, ...}``. Le ``_id`` de chaque document EST le bce10.

`db` est TOUJOURS injecté (pymongo ou mongomock) — aucun import pymongo ici.
"""
from __future__ import annotations

from . import config
from . import state_db


# ---------------------------------------------------------------------------
# Référentiels métier
# ---------------------------------------------------------------------------
# Codes NACE (NACEBEL 2008) de l'hébergement touristique — division 55.
HOTEL_NACE = {
    "55100",  # hôtels et hébergement similaire
    "55201", "55202", "55203", "55204", "55209",  # hébergement touristique court séjour
    "55300",  # terrains de camping et parcs pour caravanes
    "55400",  # auberges de jeunesse et refuges
    "55900",  # autres hébergements
}

# Formes juridiques exclues (ASBL, fondations, formes publiques / sans dépôt
# de comptes annuels pertinent pour la cible commerciale hôtelière).
EXCLUDED_FORMS = {
    "110", "114", "116", "117",
    "301", "302", "303", "310", "320", "330", "340", "350",
    "400", "411", "412", "413", "414", "415", "416", "417",
    "418", "419", "420",
}


# ---------------------------------------------------------------------------
# Sélection
# ---------------------------------------------------------------------------
def _target_collection(db, collection: str | None):
    name = collection or config.FINALE_COLLECTION
    return db[name]


def _nace_matcher() -> dict:
    """Filtre Mongo sur le NaceCode hôtellerie.

    On matche par PRÉFIXE de division NACE via ``config.HOTEL_NACE_PREFIXES``
    (défaut ``["55"]`` = toute la division 55 « Hébergement »). Un préfixe "55"
    couvre les 9 codes de l'énoncé (:data:`HOTEL_NACE`) ET leurs sous-codes
    éventuels (55101, 55102…). Mettre ``BCE_HOTEL_NACE_PREFIXES="55100,55201,..."``
    pour restreindre exactement aux codes voulus.
    """
    prefixes = config.HOTEL_NACE_PREFIXES or ["55"]
    return {"$regex": "^(" + "|".join(prefixes) + ")"}


def find_hotels(db, collection: str | None = None) -> list:
    """Renvoie les bce10 des entreprises hôtelières actives et ciblables.

    Args:
        db         : base MongoDB injectée (pymongo ou mongomock).
        collection : nom de collection source ; défaut ``config.FINALE_COLLECTION``.
                     Passer ``config.SILVER_COLLECTION`` pour cibler la Silver.

    Returns:
        Liste des ``_id`` (bce10) des entreprises satisfaisant tous les critères
        (Status AC, personne morale, forme non exclue, activité PRINCIPALE dont
        le NaceCode relève de l'hébergement — cf. :func:`_nace_matcher`).
    """
    coll = _target_collection(db, collection)
    query = {
        "Status": "AC",
        "TypeOfEnterprise": "2",
        "JuridicalForm": {"$nin": list(EXCLUDED_FORMS)},
        "activities": {
            "$elemMatch": {
                "Classification": "MAIN",
                "NaceCode": _nace_matcher(),
            }
        },
    }
    return [doc["_id"] for doc in coll.find(query, {"_id": 1})]


# ---------------------------------------------------------------------------
# Chargement dans la State DB entreprise
# ---------------------------------------------------------------------------
def load_hotels_to_state(db, collection: str | None = None) -> dict:
    """Charge les hôtels ciblés en statut ``pending`` dans la State DB entreprise.

    Idempotent : une entreprise déjà en statut ``done`` (ou ``in_progress``)
    n'est PAS réinitialisée à ``pending`` — on ne (ré)pose ``pending`` que sur
    les entreprises encore inconnues de la State DB.

    Returns:
        ``{'loaded': n}`` où n = nombre d'entreprises effectivement (re)posées
        à ``pending`` lors de cet appel.
    """
    state_db.ensure_scrape_indexes(db)
    loaded = 0
    for bce in find_hotels(db, collection=collection):
        if state_db.get_company_status(db, bce) is not None:
            # Déjà suivie (pending/in_progress/done) : ne pas écraser son statut.
            continue
        state_db.set_company_status(db, bce, "pending")
        loaded += 1
    return {"loaded": loaded}
