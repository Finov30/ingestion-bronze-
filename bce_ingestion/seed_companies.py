"""Peuplement de la collection MongoDB `companies` depuis KBO Open Data.

Source : KboOpenData .../enterprise.csv (~1,95 M lignes). Chaque ligne devient
un document `companies` idempotent, dont le `_id` est le numéro BCE à 10 chiffres
(chiffres seuls, ex. '0203430576'). L'upsert est rejouable sans effet de bord :
les champs cœur (`$set`) sont rafraîchis, tandis que l'état d'ingestion
(`$setOnInsert`) n'est posé qu'à la création.

Schéma document :
  {
    _id            : '0203430576',        # BCE 10 chiffres (= clé)
    bce_formatted  : '0203.430.576',
    status         : 'AC',
    juridical_situation : '000',
    type           : '2',
    juridical_form : '416',
    start_date     : '09-08-1960',
    denomination   : 'ACME SA',           # optionnel (--with-denomination)
    ingest_status  : 'pending',           # posé uniquement à l'insertion
    last_ingest    : None,                # idem
  }

pymongo est importé *paresseusement* (dans les fonctions) : l'environnement de
tests local a une pile pyOpenSSL cassée qui ferait échouer un import au niveau
module. Toute la logique accepte un `db` injecté (pymongo réel ou mongomock).
"""
from __future__ import annotations

import argparse
import sys

import pandas as pd

from . import config


# Colonnes du CSV enterprise et mapping vers les champs cœur du document.
_ENTERPRISE_COLS = [
    "EnterpriseNumber",
    "Status",
    "JuridicalSituation",
    "TypeOfEnterprise",
    "JuridicalForm",
    "JuridicalFormCAC",
    "StartDate",
]


def bce10(enterprise_number: str) -> str:
    """Normalise un numéro BCE en 10 chiffres (avec zéro de tête).

    '0203.430.576' -> '0203430576' ; '203430576' -> '0203430576'.
    Renvoie '' si l'entrée ne contient aucun chiffre.
    """
    if enterprise_number is None:
        return ""
    digits = "".join(ch for ch in str(enterprise_number) if ch.isdigit())
    if not digits:
        return ""
    return digits.zfill(10)


def _format_bce(bce: str) -> str:
    """'0203430576' -> '0203.430.576' (regroupement 4-3-3 comme la BCE)."""
    return f"{bce[0:4]}.{bce[4:7]}.{bce[7:10]}" if len(bce) == 10 else bce


def _clean(value) -> str | None:
    """Nettoie une valeur pandas : NaN/vide -> None, sinon str strip()."""
    if value is None:
        return None
    if isinstance(value, float) and pd.isna(value):
        return None
    s = str(value).strip()
    if s == "" or s.lower() == "nan":
        return None
    return s


def _core_from_row(row) -> tuple[str, dict] | None:
    """Construit (bce10, core_dict) depuis une ligne enterprise ; None si invalide."""
    bce = bce10(row.get("EnterpriseNumber"))
    if not bce or len(bce) != 10:
        return None
    core = {
        "bce_formatted": _format_bce(bce),
        "status": _clean(row.get("Status")),
        "juridical_situation": _clean(row.get("JuridicalSituation")),
        "type": _clean(row.get("TypeOfEnterprise")),
        "juridical_form": _clean(row.get("JuridicalForm")),
        "juridical_form_cac": _clean(row.get("JuridicalFormCAC")),
        "start_date": _clean(row.get("StartDate")),
    }
    return bce, core


def _bulk_upsert(coll, ops: list) -> tuple[int, int, int]:
    """Exécute une liste d'upserts ``(filter, update)`` et renvoie
    ``(upserted, matched, modified)``.

    Sur le cluster (pymongo + MongoDB réel) on utilise ``bulk_write`` avec des
    ``UpdateOne`` (performant sur ~1,95 M lignes). En local, mongomock 4.x ne
    supporte pas le kwarg ``sort`` que pymongo>=4.10 attache à ``UpdateOne`` : le
    ``bulk_write`` lève alors ``TypeError`` AVANT toute écriture. On retombe donc
    proprement sur des ``update_one`` unitaires (les volumes de test sont petits).
    """
    from pymongo import UpdateOne  # import paresseux (voir docstring module)

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


def _load_denominations(csv_path: str, wanted: set[str], chunksize: int) -> dict:
    """Charge les dénominations officielles FR (TypeOfDenomination='001',
    Language='1') pour les seuls BCE présents dans `wanted`.

    Le fichier denomination.csv (~3,3 M lignes) est parcouru en flux ; on ne
    conserve en mémoire que les BCE demandés — d'où la restriction à un
    sous-ensemble (les BCE effectivement seedés) documentée dans le prompt.
    Fallback : si aucune dénomination FR pour un BCE, on prend la NL ('2').
    """
    fr: dict[str, str] = {}
    nl: dict[str, str] = {}
    reader = pd.read_csv(
        csv_path, dtype=str, chunksize=chunksize,
        keep_default_na=False, na_values=[],
    )
    for chunk in reader:
        # TypeOfDenomination '001' = dénomination officielle.
        sub = chunk[chunk["TypeOfDenomination"] == "001"]
        for _, r in sub.iterrows():
            bce = bce10(r.get("EntityNumber"))
            if bce not in wanted:
                continue
            name = _clean(r.get("Denomination"))
            if name is None:
                continue
            lang = _clean(r.get("Language"))
            if lang == "1" and bce not in fr:      # 1 = français
                fr[bce] = name
            elif lang == "2" and bce not in nl:    # 2 = néerlandais (fallback)
                nl[bce] = name
    # FR prioritaire, NL en repli.
    out = dict(nl)
    out.update(fr)
    return out


def seed(db, csv_path: str = config.KBO_ENTERPRISE_CSV, limit: int | None = None,
         chunksize: int = 50000, with_denomination: bool = False,
         denomination_csv: str = config.KBO_DENOMINATION_CSV,
         batch_size: int = 10000) -> dict:
    """Peuple `db.companies` depuis enterprise.csv de façon idempotente.

    Args:
        db            : base MongoDB injectée (pymongo ou mongomock).
        csv_path      : chemin d'enterprise.csv.
        limit         : nombre max de lignes traitées (tests) ; None = tout.
        chunksize     : lignes lues par bloc pandas.
        with_denomination : si True, joint denomination.csv (FR type-001).
        denomination_csv  : chemin de denomination.csv.
        batch_size    : taille des lots bulk_write.

    Returns:
        {'processed': lignes valides traitées,
         'inserted' : documents nouvellement créés (upserted_count),
         'upserted' : documents déjà présents et rafraîchis (matched_count),
         'modified' : documents effectivement modifiés (modified_count)}
    """
    coll = db[config.COMPANIES_COLLECTION]

    # 1) Sur les gros volumes, un index unique sur _id existe déjà (clé primaire).
    #    On collecte les BCE seedés si l'on doit ensuite joindre les dénominations.
    seeded: set[str] = set() if with_denomination else set()

    processed = 0
    inserted = 0
    matched = 0
    modified = 0
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

    reader = pd.read_csv(
        csv_path, dtype=str, chunksize=chunksize,
        keep_default_na=False, na_values=[],
    )
    stop = False
    for chunk in reader:
        for _, row in chunk.iterrows():
            if limit is not None and processed >= limit:
                stop = True
                break
            built = _core_from_row(row)
            if built is None:
                continue
            bce, core = built
            if with_denomination:
                seeded.add(bce)
            ops.append((
                {"_id": bce},
                {
                    "$set": core,
                    "$setOnInsert": {"ingest_status": "pending", "last_ingest": None},
                },
            ))
            processed += 1
            if len(ops) >= batch_size:
                _flush()
        if stop:
            break
    _flush()

    # 2) Jointure optionnelle des dénominations officielles (FR type-001).
    if with_denomination and seeded:
        names = _load_denominations(denomination_csv, seeded, chunksize)
        dops = [({"_id": b}, {"$set": {"denomination": n}})
                for b, n in names.items()]
        for i in range(0, len(dops), batch_size):
            batch = dops[i:i + batch_size]
            if batch:
                _bulk_upsert(coll, batch)

    return {
        "processed": processed,
        "inserted": inserted,
        "upserted": matched,   # déjà présents (rafraîchis) — utile pour l'idempotence
        "modified": modified,
    }


def _main(argv=None) -> int:
    parser = argparse.ArgumentParser(
        description="Peuple la collection MongoDB 'companies' depuis KBO enterprise.csv.")
    parser.add_argument("--csv", default=config.KBO_ENTERPRISE_CSV,
                        help="Chemin d'enterprise.csv.")
    parser.add_argument("--limit", type=int, default=None,
                        help="Nombre max de lignes (tests).")
    parser.add_argument("--chunksize", type=int, default=50000,
                        help="Lignes par bloc pandas.")
    parser.add_argument("--batch-size", type=int, default=10000,
                        help="Taille des lots bulk_write.")
    parser.add_argument("--with-denomination", action="store_true",
                        help="Joint denomination.csv (dénomination officielle FR type-001).")
    parser.add_argument("--denomination-csv", default=config.KBO_DENOMINATION_CSV,
                        help="Chemin de denomination.csv.")
    args = parser.parse_args(argv)

    from . import mongo  # import paresseux (dépend de pymongo)
    db = mongo.get_db()

    result = seed(
        db,
        csv_path=args.csv,
        limit=args.limit,
        chunksize=args.chunksize,
        with_denomination=args.with_denomination,
        denomination_csv=args.denomination_csv,
        batch_size=args.batch_size,
    )
    total = db[config.COMPANIES_COLLECTION].count_documents({})
    print("Seed terminé :", result)
    print("Total documents 'companies' :", total)
    return 0


if __name__ == "__main__":
    sys.exit(_main())
