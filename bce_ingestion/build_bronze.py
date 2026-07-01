"""Construction de la couche Bronze « riche » (collection ``enterprise_finale``).

JOUR 2 — medallion. On assemble, par entreprise, les données KBO Open Data en
un unique document imbriqué (entreprise + dénominations + adresses + activités),
clé primaire ``_id = bce10`` (numéro BCE à 10 chiffres, ex. '0203430576').

Schéma document :
  {
    _id                : '0203430576',        # BCE 10 chiffres (= clé)
    EnterpriseNumber   : '0203.430.576',      # forme pointée d'origine
    Status, JuridicalSituation, TypeOfEnterprise,
    JuridicalForm, JuridicalFormCAC, StartDate,
    denominations : [{Language, TypeOfDenomination, Denomination}],
    addresses     : [{TypeOfAddress, Zipcode, MunicipalityFR, StreetFR,
                      HouseNumber, Box, CountryFR}],
    activities    : [{ActivityGroup, NaceVersion, NaceCode, Classification}],
  }

Les fichiers KBO référencent l'entreprise par ``EntityNumber`` (forme pointée,
ex. '0203.430.576'), identique à ``EnterpriseNumber`` du fichier enterprise. La
jointure se fait donc sur cette chaîne pointée (normalisée en bce10 pour la clé).

pymongo est importé *paresseusement* dans les fonctions (pile pyOpenSSL locale
cassée) ; toute la logique accepte un ``db`` injecté (pymongo réel ou mongomock).
"""
from __future__ import annotations

import argparse
import os
import sys

import pandas as pd

from . import config

# Réutilisation des helpers de normalisation/nettoyage de Jour 1.
from .seed_companies import bce10, _clean


# ---------------------------------------------------------------------------
# Bulk upsert tolérant pymongo / mongomock (repris de seed_companies).
# ---------------------------------------------------------------------------
def _bulk_upsert(coll, ops: list) -> tuple[int, int, int]:
    """Exécute des upserts ``(filter, update)`` -> ``(upserted, matched, modified)``.

    Sur le cluster (pymongo + MongoDB) on utilise ``bulk_write`` avec des
    ``UpdateOne`` (performant). En local, mongomock ne supporte pas le kwarg
    ``sort`` que pymongo>=4.10 attache à ``UpdateOne`` : le ``bulk_write`` lève
    ``TypeError`` avant toute écriture, on retombe alors sur des ``update_one``.
    """
    from pymongo import UpdateOne  # import paresseux

    try:
        res = coll.bulk_write(
            [UpdateOne(f, u, upsert=True) for (f, u) in ops], ordered=False
        )
        return res.upserted_count, res.matched_count, res.modified_count
    except TypeError:
        up = ma = mo = 0
        for (f, u) in ops:
            r = coll.update_one(f, u, upsert=True)
            if r.upserted_id is not None:
                up += 1
            else:
                ma += r.matched_count
                mo += r.modified_count
        return up, ma, mo


def _default_kbo_dir() -> str:
    """Dossier KBO déduit du chemin d'enterprise.csv de la config."""
    return os.path.dirname(config.KBO_ENTERPRISE_CSV)


def _reader(csv_path: str, chunksize: int):
    """Lecteur pandas en flux, tout en chaîne, sans interprétation NA."""
    return pd.read_csv(
        csv_path, dtype=str, chunksize=chunksize,
        keep_default_na=False, na_values=[],
    )


# ---------------------------------------------------------------------------
# Collecte des lignes enfant (dénomination / adresse / activité) par entreprise.
# ---------------------------------------------------------------------------
# (csv, colonnes conservées) pour chaque type d'enfant.
_DENOM_COLS = ["Language", "TypeOfDenomination", "Denomination"]
_ADDR_COLS  = ["TypeOfAddress", "Zipcode", "MunicipalityFR", "StreetFR",
               "HouseNumber", "Box", "CountryFR"]
_ACT_COLS   = ["ActivityGroup", "NaceVersion", "NaceCode", "Classification"]


def _collect_children(csv_path: str, cols: list[str], wanted: set[str] | None,
                      chunksize: int) -> dict[str, list[dict]]:
    """Regroupe les lignes enfant par ``bce10``.

    Ne conserve en mémoire que les entreprises de ``wanted`` (ou toutes si
    ``wanted is None``). Chaque ligne est projetée sur ``cols`` (valeurs
    nettoyées : NaN/vide -> None).
    """
    out: dict[str, list[dict]] = {}
    for chunk in _reader(csv_path, chunksize):
        # Sélection colonnaire (certaines colonnes peuvent manquer selon le dump).
        present = [c for c in cols if c in chunk.columns]
        for row in chunk.itertuples(index=False):
            d = row._asdict()
            bce = bce10(d.get("EntityNumber"))
            if not bce or (wanted is not None and bce not in wanted):
                continue
            out.setdefault(bce, []).append(
                {c: _clean(d.get(c)) for c in present}
            )
    return out


def _select_wanted(enterprise_csv: str, limit: int, chunksize: int) -> set[str]:
    """Retourne les ``limit`` premiers bce10 valides d'enterprise.csv."""
    wanted: set[str] = set()
    for chunk in _reader(enterprise_csv, chunksize):
        for num in chunk["EnterpriseNumber"]:
            bce = bce10(num)
            if bce and len(bce) == 10:
                wanted.add(bce)
                if len(wanted) >= limit:
                    return wanted
    return wanted


# ---------------------------------------------------------------------------
# Construction principale.
# ---------------------------------------------------------------------------
def build_enterprise_finale(db, bces=None, kbo_dir: str | None = None,
                            limit: int | None = None,
                            chunksize: int = 100000,
                            batch_size: int = 5000) -> dict:
    """Construit la collection Bronze riche ``enterprise_finale``.

    Args:
        db        : base injectée (pymongo réel ou mongomock).
        bces      : itérable de BCE (10 chiffres OU pointés) à construire
                    exclusivement ; ``None`` = toutes les entreprises.
        kbo_dir   : dossier des CSV KBO ; défaut = dirname(config.KBO_ENTERPRISE_CSV).
        limit     : nombre max d'entreprises (ignoré si ``bces`` fourni).
        chunksize : lignes lues par bloc pandas.
        batch_size: taille des lots d'upsert.

    Returns:
        {'companies', 'inserted', 'matched', 'modified',
         'denominations', 'addresses', 'activities'}
    """
    kbo_dir = kbo_dir or _default_kbo_dir()
    enterprise_csv   = os.path.join(kbo_dir, "enterprise.csv")
    denomination_csv = os.path.join(kbo_dir, "denomination.csv")
    address_csv      = os.path.join(kbo_dir, "address.csv")
    activity_csv     = os.path.join(kbo_dir, "activity.csv")

    coll = db[config.FINALE_COLLECTION]

    # 1) Déterminer l'ensemble ciblé (bce10) -----------------------------------
    if bces is not None:
        wanted: set[str] | None = {b for b in (bce10(x) for x in bces) if b}
        if not wanted:
            return {"companies": 0, "inserted": 0, "matched": 0, "modified": 0,
                    "denominations": 0, "addresses": 0, "activities": 0}
    elif limit is not None:
        wanted = _select_wanted(enterprise_csv, limit, chunksize)
    else:
        wanted = None  # toutes

    # 2) Collecter les enfants pour l'ensemble ciblé ---------------------------
    denoms = _collect_children(denomination_csv, _DENOM_COLS, wanted, chunksize)
    addrs  = _collect_children(address_csv,      _ADDR_COLS,  wanted, chunksize)
    acts   = _collect_children(activity_csv,     _ACT_COLS,   wanted, chunksize)

    n_denoms = sum(len(v) for v in denoms.values())
    n_addrs  = sum(len(v) for v in addrs.values())
    n_acts   = sum(len(v) for v in acts.values())

    # 3) Streamer enterprise.csv, assembler et upserter ------------------------
    companies = 0
    inserted = matched = modified = 0
    ops: list = []

    def _flush():
        nonlocal inserted, matched, modified, ops
        if not ops:
            return
        up, ma, mo = _bulk_upsert(coll, ops)
        inserted += up
        matched += ma
        modified += mo
        ops = []

    for chunk in _reader(enterprise_csv, chunksize):
        for row in chunk.itertuples(index=False):
            d = row._asdict()
            bce = bce10(d.get("EnterpriseNumber"))
            if not bce or len(bce) != 10:
                continue
            if wanted is not None and bce not in wanted:
                continue
            doc = {
                "EnterpriseNumber":   _clean(d.get("EnterpriseNumber")),
                "Status":             _clean(d.get("Status")),
                "JuridicalSituation": _clean(d.get("JuridicalSituation")),
                "TypeOfEnterprise":   _clean(d.get("TypeOfEnterprise")),
                "JuridicalForm":      _clean(d.get("JuridicalForm")),
                "JuridicalFormCAC":   _clean(d.get("JuridicalFormCAC")),
                "StartDate":          _clean(d.get("StartDate")),
                "denominations":      denoms.get(bce, []),
                "addresses":          addrs.get(bce, []),
                "activities":         acts.get(bce, []),
            }
            ops.append(({"_id": bce}, {"$set": doc}))
            companies += 1
            if len(ops) >= batch_size:
                _flush()
    _flush()

    return {
        "companies": companies,
        "inserted": inserted,
        "matched": matched,
        "modified": modified,
        "denominations": n_denoms,
        "addresses": n_addrs,
        "activities": n_acts,
    }


def _main(argv=None) -> int:
    parser = argparse.ArgumentParser(
        description="Construit la collection Bronze riche 'enterprise_finale' "
                    "depuis les CSV KBO (enterprise + denomination + address + activity).")
    parser.add_argument("--limit", type=int, default=None,
                        help="Nombre max d'entreprises (tests).")
    parser.add_argument("--bces", default=None,
                        help="Liste de BCE (séparés par des virgules) à construire "
                             "exclusivement (pointés ou 10 chiffres).")
    parser.add_argument("--kbo-dir", default=None,
                        help="Dossier des CSV KBO (défaut : dirname de config.KBO_ENTERPRISE_CSV).")
    parser.add_argument("--chunksize", type=int, default=100000,
                        help="Lignes par bloc pandas.")
    args = parser.parse_args(argv)

    bces = None
    if args.bces:
        bces = [b for b in (x.strip() for x in args.bces.split(",")) if b]

    from . import mongo  # import paresseux (dépend de pymongo)
    db = mongo.get_db()

    result = build_enterprise_finale(
        db, bces=bces, kbo_dir=args.kbo_dir,
        limit=args.limit, chunksize=args.chunksize,
    )
    total = db[config.FINALE_COLLECTION].count_documents({})
    print("Build terminé :", result)
    print("Total documents 'enterprise_finale' :", total)
    return 0


if __name__ == "__main__":
    sys.exit(_main())
