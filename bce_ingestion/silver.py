"""Couche Silver — nettoyage/enrichissement du Bronze ``enterprise_finale``.

On lit les documents « riches » de ``config.FINALE_COLLECTION`` (entreprises avec
activités / adresses / dénominations imbriquées) et on écrit des documents
propres dans ``config.SILVER_COLLECTION``, upsertés par ``_id`` (idempotent :
rejouer ``build_silver`` ne crée aucun doublon).

La Bronze ``enterprise_finale`` a UN document par entreprise, ``_id`` = bce10, aux
champs à la casse KBO et à tableaux imbriqués ::

    {
      _id: '0203430576', EnterpriseNumber: '0203.430.576',
      Status: 'AC', JuridicalSituation: '000', TypeOfEnterprise: '2',
      JuridicalForm: '416', StartDate: '16-03-1880',
      denominations: [ {Language, TypeOfDenomination, Denomination}, ... ],
      addresses:     [ {TypeOfAddress, Zipcode, MunicipalityFR, ...}, ... ],
      activities:    [ {ActivityGroup, NaceVersion, NaceCode, Classification}, ... ],
    }

Cinq règles (énoncé Jour 2)
---------------------------
1. **StartDate** ``DD-MM-YYYY`` -> ``YYYY-MM-DD`` (chaîne). Vide/malformé -> ``None``.
2. **Activités** : unicité par ``(NaceCode, Classification)`` EXACTE — ``70220``
   et ``70200`` sont distincts, ``MAIN`` et ``SECO`` sont conservés tous les
   deux ; on ne supprime que les vrais doublons (même NaceCode ET même
   Classification à travers les versions NACE), en gardant la version la plus
   récente (2025 > 2008 > 2003).
3. **Adresse** : on ne garde que ``TypeOfAddress == 'REGO'`` (siège) — un seul
   objet ``address`` (ou ``None``).
4. **Dénomination** : ``TypeOfDenomination == '1'`` = nom officiel ->
   ``denomination_principale`` + placé en tête de ``denominations``.
5. **Libellés** : on GARDE les codes bruts et on AJOUTE ``StatusLabel``,
   ``JuridicalFormLabel`` (+ ``JuridicalSituationLabel`` / ``TypeOfEnterpriseLabel``)
   et ``activities[].NaceLabel`` (FR, via ``codes.nace_label``).

Les fonctions acceptent un ``db`` injecté (pymongo ou mongomock). L'écriture
Silver passe par ``bulk_write``/``replace_one`` (upsert) — aucun import pymongo
n'est requis en local (repli si indisponible).
"""
from __future__ import annotations

from datetime import datetime

from . import config
from .codes import load_codes, label, nace_label


# ---------------------------------------------------------------------------
# Petits utilitaires d'accès tolérants (Bronze en clés KBO ou minuscules)
# ---------------------------------------------------------------------------
def _get(doc, *keys, default=None):
    """Première valeur non vide parmi ``keys`` (accepte variantes de casse)."""
    if not isinstance(doc, dict):
        return default
    for key in keys:
        if key in doc:
            val = doc[key]
            if val is not None and val != "":
                return val
    return default


def _get_list(doc, *keys) -> list:
    """Première valeur de type ``list`` parmi ``keys`` (sinon ``[]``)."""
    if not isinstance(doc, dict):
        return []
    for key in keys:
        val = doc.get(key)
        if isinstance(val, list):
            return val
    return []


# ---------------------------------------------------------------------------
# Règle 1 — normalisation de la date
# ---------------------------------------------------------------------------
def _normalize_date(value) -> str | None:
    """``'16-03-1880'`` -> ``'1880-03-16'``. ``None``/vide/malformé -> ``None``.

    Accepte aussi les formats déjà ISO (``YYYY-MM-DD``) et ``DD/MM/YYYY``.
    """
    if value is None:
        return None
    s = str(value).strip()
    if not s:
        return None
    for fmt in ("%d-%m-%Y", "%Y-%m-%d", "%d/%m/%Y"):
        try:
            return datetime.strptime(s, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return None


# ---------------------------------------------------------------------------
# Règle 2 — dédoublonnage des activités
# ---------------------------------------------------------------------------
def _version_rank(nace_version) -> int:
    """Rang numérique d'une version NACE (2025 > 2008 > 2003). Inconnu -> 0."""
    digits = "".join(ch for ch in str(nace_version or "") if ch.isdigit())
    return int(digits) if digits else 0


def _dedup_activities(activities) -> list:
    """Unicité par ``(NaceCode, Classification)`` ; garde la version la + récente.

    Préserve l'ordre de première apparition des clés distinctes. ``70220`` et
    ``70200`` restent séparés (NaceCode différent) ; ``MAIN`` et ``SECO`` restent
    séparés (Classification différente). Seuls les vrais doublons (même NaceCode
    ET même Classification) sont fusionnés en gardant la version la plus récente.
    """
    best: dict = {}
    order: list = []
    for act in activities or []:
        nace = _get(act, "NaceCode", "nace_code")
        classif = _get(act, "Classification", "classification")
        key = (nace, classif)
        rank = _version_rank(_get(act, "NaceVersion", "nace_version"))
        if key not in best:
            best[key] = (rank, act)
            order.append(key)
        elif rank > best[key][0]:
            best[key] = (rank, act)
    return [best[key][1] for key in order]


def _enrich_activity(act, codes) -> dict:
    """Activité nettoyée + ``NaceLabel`` (FR) ajouté, codes bruts conservés."""
    nace = _get(act, "NaceCode", "nace_code")
    return {
        "ActivityGroup": _get(act, "ActivityGroup", "activity_group"),
        "NaceVersion": _get(act, "NaceVersion", "nace_version"),
        "NaceCode": nace,
        "Classification": _get(act, "Classification", "classification"),
        "NaceLabel": nace_label(codes, nace),
    }


# ---------------------------------------------------------------------------
# Règle 3 — adresse du siège (REGO) uniquement
# ---------------------------------------------------------------------------
def _pick_rego(addresses) -> dict | None:
    """Renvoie la seule adresse ``TypeOfAddress == 'REGO'`` (ou ``None``)."""
    for addr in addresses or []:
        type_of = _get(addr, "TypeOfAddress", "type_of_address")
        if str(type_of or "") == "REGO":
            return dict(addr)
    return None


# ---------------------------------------------------------------------------
# Règle 4 — dénominations (officielle d'abord)
# ---------------------------------------------------------------------------
def _is_official_denomination(type_of_denomination) -> bool:
    """``'1'`` (ou variante KBO ``'001'``) = dénomination officielle."""
    return str(type_of_denomination or "") in ("1", "001")


def _order_denominations(denominations):
    """(liste officielle-d'abord, nom principal). ``principale`` = 1er officiel."""
    officials: list = []
    others: list = []
    for den in denominations or []:
        type_of = _get(den, "TypeOfDenomination", "type_of_denomination")
        (officials if _is_official_denomination(type_of) else others).append(dict(den))
    ordered = officials + others
    principale = None
    if officials:
        principale = _get(officials[0], "Denomination", "denomination")
    return ordered, principale


# ---------------------------------------------------------------------------
# Transformation d'un document
# ---------------------------------------------------------------------------
def to_silver(doc: dict, codes: dict) -> dict:
    """Transforme un document Bronze ``enterprise_finale`` en document Silver."""
    out = dict(doc)  # conserve _id + champs bruts (codes d'origine gardés)

    # Règle 1 — date normalisée (on garde l'original sous StartDate_raw).
    raw_date = _get(doc, "StartDate", "start_date")
    if raw_date is not None:
        out["StartDate_raw"] = raw_date
    out["StartDate"] = _normalize_date(raw_date)
    out.pop("start_date", None)

    # Règle 2 + 5 — activités dédoublonnées et enrichies (NaceLabel).
    activities = _dedup_activities(_get_list(doc, "activities"))
    out["activities"] = [_enrich_activity(a, codes) for a in activities]

    # Règle 3 — adresse du siège uniquement.
    out["address"] = _pick_rego(_get_list(doc, "addresses"))
    out.pop("addresses", None)

    # Règle 4 — dénominations (officielle en tête) + principale.
    denominations, principale = _order_denominations(_get_list(doc, "denominations"))
    out["denominations"] = denominations
    out["denomination_principale"] = principale

    # Règle 5 — libellés décodés (codes bruts conservés).
    out["StatusLabel"] = label(codes, "Status", _get(doc, "Status", "status"))
    out["JuridicalFormLabel"] = label(
        codes, "JuridicalForm", _get(doc, "JuridicalForm", "juridical_form")
    )
    out["JuridicalSituationLabel"] = label(
        codes, "JuridicalSituation", _get(doc, "JuridicalSituation", "juridical_situation")
    )
    out["TypeOfEnterpriseLabel"] = label(
        codes, "TypeOfEnterprise", _get(doc, "TypeOfEnterprise", "type")
    )

    return out


# ---------------------------------------------------------------------------
# Écriture Silver (upsert idempotent par _id)
# ---------------------------------------------------------------------------
def _bulk_replace(coll, ops: list) -> None:
    """Upsert d'un lot ``[(filter, doc), ...]`` ; repli unitaire si besoin.

    Sur le cluster, ``bulk_write`` de ``ReplaceOne`` est performant. En local, ou
    si pymongo/mongomock rejette le bulk, on retombe sur ``replace_one``.
    """
    if not ops:
        return
    try:
        from pymongo import ReplaceOne

        coll.bulk_write(
            [ReplaceOne(flt, doc, upsert=True) for (flt, doc) in ops],
            ordered=False,
        )
    except (ImportError, TypeError):
        for flt, doc in ops:
            coll.replace_one(flt, doc, upsert=True)


def build_silver(db, code_csv: str = config.KBO_CODE_CSV, batch_size: int = 1000) -> dict:
    """Construit la couche Silver depuis Bronze ``enterprise_finale``.

    Lit ``config.FINALE_COLLECTION``, applique ``to_silver`` et upserte dans
    ``config.SILVER_COLLECTION`` par ``_id``. Rejouable sans doublon.

    Returns:
        ``{'transformed': n}`` — nombre de documents transformés/upsertés.
    """
    codes = load_codes(code_csv)
    src = db[config.FINALE_COLLECTION]
    dst = db[config.SILVER_COLLECTION]

    transformed = 0
    ops: list = []
    for doc in src.find({}):
        silver = to_silver(doc, codes)
        ops.append(({"_id": silver.get("_id")}, silver))
        transformed += 1
        if len(ops) >= batch_size:
            _bulk_replace(dst, ops)
            ops = []
    _bulk_replace(dst, ops)

    return {"transformed": transformed}


# ---------------------------------------------------------------------------
# CLI (jamais exécutée à l'import)
# ---------------------------------------------------------------------------
def _main(argv=None) -> int:
    import argparse

    parser = argparse.ArgumentParser(
        description="Construit la couche Silver depuis Bronze enterprise_finale."
    )
    parser.add_argument("--code-csv", default=config.KBO_CODE_CSV)
    parser.add_argument("--batch-size", type=int, default=1000)
    args = parser.parse_args(argv)

    from . import mongo  # import paresseux (dépend de pymongo)

    db = mongo.get_db()
    result = build_silver(db, code_csv=args.code_csv, batch_size=args.batch_size)
    total = db[config.SILVER_COLLECTION].count_documents({})
    print("Silver construit :", result)
    print("Total documents Silver :", total)
    return 0


if __name__ == "__main__":
    import sys

    sys.exit(_main())
