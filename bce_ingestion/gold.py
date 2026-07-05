"""Couche GOLD — agrégats financiers annuels par entreprise (``hotel_gold``).

On lit les dépôts comptables BNB (CSV « réels », format ci-dessous), on en
extrait les grandeurs du PCMN belge (chiffre d'affaires, achats, EBIT, résultat
net, trésorerie, dettes financières, fonds propres, capital souscrit), on calcule
un jeu de ratios par exercice, puis on upserte un document par entreprise dans
``config.GOLD_COLLECTION`` (``hotel_gold``), clé ``enterprise_number`` (= bce10).

Format CSV BNB réel (cf. énoncé Jour 3)
---------------------------------------
CSV séparé par des virgules, TOUS les champs entre guillemets, SANS en-tête.
Les ~18 premières lignes sont des paires de métadonnées ::

    "Reference number","2025-00184871"
    "Entity number","0878065378"
    "Accounting period end date","2024-12-31"
    "Model code","m02-f"
    "Legal form","..."

Puis viennent les lignes de données ``"CODE_PCMN","valeur"`` ::

    "70","92512760.63"
    "10/15","65427184.88"

Règles de parsing :
- une ligne est une **donnée** si sa clé commence par un chiffre (les codes PCMN
  commencent tous par un chiffre : ``70``, ``60/66A``, ``10/15``… ; les libellés
  de métadonnées commencent tous par une lettre) ;
- les codes se terminant par ``P`` sont les **comparatifs N-1** → on les IGNORE ;
- ``Model code`` → ``schema_type`` (full/abrege/micro) ;
- ``Accounting period end date`` → année de l'exercice (year).

Les fonctions « pures » (parse/mapping/ratios) ne dépendent d'aucune base ni de
Spark : elles sont l'unique source de vérité de la logique financière, réutilisée
telle quelle par ``gold_spark.py``. L'écriture Mongo reçoit un ``db`` injecté
(pymongo OU mongomock) et n'importe ``pymongo`` que paresseusement.
"""
from __future__ import annotations

import csv
import io
from datetime import datetime, timezone

from . import config


# ---------------------------------------------------------------------------
# Utilitaires numériques
# ---------------------------------------------------------------------------
def _to_float(value) -> float:
    """Convertit une valeur BNB en ``float`` (gère décimales ``,`` ou ``.``).

    Exemples : ``"810629.03"`` -> 810629.03 ; ``"1.234,56"`` -> 1234.56 ;
    ``"12,5"`` -> 12.5 ; vide/illisible -> 0.0.
    """
    if value is None:
        return 0.0
    if isinstance(value, (int, float)):
        return float(value)
    s = str(value).strip().replace(" ", "").replace(" ", "")
    if not s:
        return 0.0
    if "," in s and "." in s:
        # Le séparateur décimal est le dernier symbole rencontré.
        if s.rfind(",") > s.rfind("."):
            s = s.replace(".", "").replace(",", ".")   # 1.234,56 -> 1234.56
        else:
            s = s.replace(",", "")                       # 1,234.56 -> 1234.56
    elif "," in s:
        s = s.replace(",", ".")                          # 12,5 -> 12.5
    try:
        return float(s)
    except ValueError:
        return 0.0


def _safe_div(numerator: float, denominator: float):
    """Division protégée : dénominateur nul/None -> ``None``."""
    if not denominator:
        return None
    return numerator / denominator


# ---------------------------------------------------------------------------
# Métadonnées : Model code -> schema_type, date -> année
# ---------------------------------------------------------------------------
def schema_type_from_model(model_code) -> str:
    """``Model code`` BNB -> ``'full' | 'abrege' | 'micro'``.

    m01/m81 -> full ; m02/m82 -> abrege ; m11/m12/micro -> micro ; sinon abrege.
    Tolère les suffixes (``m02-f`` -> ``m02``) et la casse.
    """
    mc = str(model_code or "").strip().lower()
    core = mc.split("-")[0].strip()
    if "micro" in mc or core in ("m11", "m12"):
        return "micro"
    if core in ("m01", "m81"):
        return "full"
    if core in ("m02", "m82"):
        return "abrege"
    return "abrege"


def _year_from_date(value):
    """``"2024-12-31"`` -> 2024 ; formats tolérés ; sinon ``None``."""
    if value is None:
        return None
    s = str(value).strip()
    if not s:
        return None
    for fmt in ("%Y-%m-%d", "%d-%m-%Y", "%d/%m/%Y", "%Y/%m/%d"):
        try:
            return datetime.strptime(s, fmt).year
        except ValueError:
            continue
    # Repli : premier bloc de 4 chiffres plausible.
    import re
    m = re.search(r"(19|20)\d{2}", s)
    return int(m.group(0)) if m else None


# ---------------------------------------------------------------------------
# Parsing d'un CSV BNB
# ---------------------------------------------------------------------------
def parse_pcmn(csv_text: str) -> dict:
    """Parse un CSV de dépôt BNB.

    Returns:
        ``{'year': int|None, 'schema_type': str, 'codes': {code: float}}`` où
        ``codes`` exclut les métadonnées ET les lignes comparatives (``…P``).
    """
    year = None
    model_code = None
    codes: dict = {}

    reader = csv.reader(io.StringIO(csv_text or ""))
    for row in reader:
        if not row:
            continue
        key = (row[0] or "").strip()
        if not key:
            continue
        value = row[1].strip() if len(row) > 1 else ""

        # Ligne de DONNÉE : la clé commence par un chiffre (code PCMN).
        if key[0].isdigit():
            if key.endswith("P"):        # comparatif N-1 -> on ignore
                continue
            codes[key] = _to_float(value)
            continue

        # Sinon : métadonnée.
        low = key.lower()
        if low == "accounting period end date":
            year = _year_from_date(value)
        elif low == "model code":
            model_code = value

    return {"year": year, "schema_type": schema_type_from_model(model_code), "codes": codes}


# ---------------------------------------------------------------------------
# Mapping PCMN -> grandeurs GOLD
# ---------------------------------------------------------------------------
def _code(codes: dict, name: str) -> float:
    """Valeur d'un code PCMN (0.0 si absent)."""
    return float(codes.get(name, 0.0) or 0.0)


def _fonds_propres(codes: dict) -> float:
    """Fonds propres = code ``10/15`` ; à défaut somme des codes ``10``..``15``."""
    if "10/15" in codes:
        return _code(codes, "10/15")
    return sum(_code(codes, str(c)) for c in range(10, 16))


def gold_fields(codes: dict) -> dict:
    """Applique le mapping PCMN de l'énoncé (codes manquants -> 0.0).

    70->ca ; 60->achats ; 71->variation_stocks ; 9901->ebit ; 9904->resultat_net ;
    trésorerie = 54+55 ; dettes_financieres = 17+43 ; fonds_propres = 10/15
    (repli somme 10..15) ; 100->capital_souscrit.
    """
    return {
        "ca": _code(codes, "70"),
        "achats": _code(codes, "60"),
        "variation_stocks": _code(codes, "71"),
        "ebit": _code(codes, "9901"),
        "resultat_net": _code(codes, "9904"),
        "tresorerie": _code(codes, "54") + _code(codes, "55"),
        "dettes_financieres": _code(codes, "17") + _code(codes, "43"),
        "fonds_propres": _fonds_propres(codes),
        "capital_souscrit": _code(codes, "100"),
    }


# ---------------------------------------------------------------------------
# Ratios
# ---------------------------------------------------------------------------
def compute_ratios(f: dict) -> dict:
    """Ratios par exercice (divisions par zéro -> ``None``).

    marge_nette(%) = resultat_net/ca*100 ; roe(%) = resultat_net/fonds_propres*100 ;
    ratio_liquidite = tresorerie/dettes_financieres ;
    taux_endettement(%) = dettes_financieres/fonds_propres*100.
    (``marge_brute`` = ca - achats + variation_stocks est un CHAMP, pas un ratio.)
    """
    marge_nette = _safe_div(f["resultat_net"], f["ca"])
    roe = _safe_div(f["resultat_net"], f["fonds_propres"])
    taux_endettement = _safe_div(f["dettes_financieres"], f["fonds_propres"])
    return {
        "marge_nette": None if marge_nette is None else marge_nette * 100.0,
        "roe": None if roe is None else roe * 100.0,
        "ratio_liquidite": _safe_div(f["tresorerie"], f["dettes_financieres"]),
        "taux_endettement": None if taux_endettement is None else taux_endettement * 100.0,
    }


def marge_brute(f: dict) -> float:
    """Marge brute = chiffre d'affaires - achats + variation des stocks."""
    return f["ca"] - f["achats"] + f["variation_stocks"]


# ---------------------------------------------------------------------------
# Entrée annuelle & document entreprise
# ---------------------------------------------------------------------------
def build_year_entry(csv_text: str) -> dict:
    """Un CSV BNB -> une entrée annuelle GOLD (champs + marge_brute + ratios).

    L'entrée porte aussi ``schema_type`` (utile pour agréger au niveau doc).
    """
    parsed = parse_pcmn(csv_text)
    fields = gold_fields(parsed["codes"])
    entry = {
        "year": parsed["year"],
        "ca": fields["ca"],
        "marge_brute": marge_brute(fields),
        "ebit": fields["ebit"],
        "resultat_net": fields["resultat_net"],
        "tresorerie": fields["tresorerie"],
        "dettes_financieres": fields["dettes_financieres"],
        "fonds_propres": fields["fonds_propres"],
        "capital_souscrit": fields["capital_souscrit"],
        "ratios": compute_ratios(fields),
        "schema_type": parsed["schema_type"],
    }
    return entry


def _bce10(value) -> str:
    """Normalise un numéro BCE en 10 chiffres (``878065378`` -> ``0878065378``)."""
    digits = "".join(ch for ch in str(value or "") if ch.isdigit())
    return digits.zfill(10) if digits else str(value or "")


def build_gold_for_company(bce, csv_texts) -> dict:
    """Construit le document ``hotel_gold`` d'une entreprise à partir de ses CSV.

    - une entrée par exercice, triée par année croissante ;
    - si plusieurs dépôts couvrent la même année, le DERNIER rencontré gagne ;
    - ``schema_type`` du document = celui de l'exercice le plus récent ;
    - les CSV sans année exploitable sont ignorés.
    """
    number = _bce10(bce)
    by_year: dict = {}
    for text in csv_texts or []:
        entry = build_year_entry(text)
        if entry["year"] is None:
            continue
        by_year[entry["year"]] = entry

    years_sorted = sorted(by_year)
    years = []
    for y in years_sorted:
        e = dict(by_year[y])
        e.pop("schema_type", None)   # le schema_type vit au niveau du document
        years.append(e)

    schema_type = by_year[years_sorted[-1]]["schema_type"] if years_sorted else "abrege"

    return {
        "_id": number,
        "enterprise_number": number,
        "years": years,
        "schema_type": schema_type,
        "last_updated": datetime.now(timezone.utc),
    }


# ---------------------------------------------------------------------------
# Découverte des CSV (dossier local, mapping, ou callable) & écriture Mongo
# ---------------------------------------------------------------------------
def _read_text(path) -> str:
    with open(path, "r", encoding="utf-8", errors="replace") as fh:
        return fh.read()


def _iter_company_csvs(csv_source):
    """Yield ``(bce, [csv_text, ...])`` depuis une source flexible.

    ``csv_source`` peut être :
    - un **dossier** (str/``Path``) contenant un sous-dossier par entreprise. On
      collecte récursivement tous les ``*.csv`` sous ``{dir}/{bce}/`` — ce qui
      couvre AUSSI BIEN la disposition « plate » ``{bce}/{year}.csv`` que la
      disposition Bronze imbriquée ``{bce}/nbb/{year}/*.csv`` (ou ``{bce}/nbb/{year}.csv``) ;
    - un **dict** ``{bce: [csv_text, ...]}`` (textes déjà chargés) ;
    - un **callable** sans argument renvoyant un tel dict / itérable de paires.

    (La lecture directe depuis HDFS est prise en charge par ``gold_spark.py`` ;
    en local on pointe simplement ``csv_source`` sur un dossier.)
    """
    # callable -> on l'appelle pour obtenir dict / itérable de paires
    if callable(csv_source):
        csv_source = csv_source()

    # dict {bce: [textes]}
    if isinstance(csv_source, dict):
        for bce, texts in csv_source.items():
            yield bce, list(texts)
        return

    # itérable de paires (bce, [textes]) déjà prêt
    if not isinstance(csv_source, (str, bytes)) and hasattr(csv_source, "__iter__"):
        # On distingue « chemin » de « itérable de paires » : un str est un chemin.
        from pathlib import Path
        if not isinstance(csv_source, Path):
            for bce, texts in csv_source:
                yield bce, list(texts)
            return

    # dossier local
    from pathlib import Path
    base = Path(csv_source)
    for bce_dir in sorted(base.iterdir()):
        if not bce_dir.is_dir():
            continue
        csv_paths = sorted(bce_dir.rglob("*.csv"))
        if not csv_paths:
            continue
        yield bce_dir.name, [_read_text(p) for p in csv_paths]


def build_gold(db, csv_source) -> dict:
    """Construit/rafraîchit la couche GOLD ``hotel_gold`` (idempotent).

    Regroupe les CSV par entreprise (``_iter_company_csvs``), construit le document
    via ``build_gold_for_company`` et upserte sur ``enterprise_number`` dans
    ``config.GOLD_COLLECTION``. Rejouable sans doublon.

    Returns:
        ``{'companies': n, 'years': n}`` — nb d'entreprises upsertées et total
        d'entrées annuelles écrites.
    """
    coll = db[config.GOLD_COLLECTION]
    companies = 0
    total_years = 0
    for bce, texts in _iter_company_csvs(csv_source):
        doc = build_gold_for_company(bce, texts)
        if not doc["years"]:
            continue
        coll.replace_one({"enterprise_number": doc["enterprise_number"]}, doc, upsert=True)
        companies += 1
        total_years += len(doc["years"])
    return {"companies": companies, "years": total_years}


def build_gold_from_bronze(db, hdfs, bce) -> dict:
    """Recalcule la couche Gold d'UNE entreprise depuis le Bronze HDFS.

    Lit les CSV sous ``{bce}/nbb/`` via ``hdfs`` (``bce_ingestion.hdfs_io.HdfsIO``)
    puis délègue à :func:`build_gold`. Utilisé par le DAG de recalcul annuel
    (``bce_gold_recalc``) pour ne retraiter qu'une entreprise à la fois.

    Returns:
        ``{'companies': 0|1, 'years': n}``.
    """
    number = _bce10(bce)
    # Le pipeline écrit le Bronze sous {bce10}/nbb/ ; par robustesse on tente
    # aussi la forme sans zéro de tête si le dossier padé est vide.
    rels = hdfs.list_files(f"{number}/nbb", suffix=".csv")
    if not rels:
        short = number.lstrip("0")
        if short and short != number:
            rels = hdfs.list_files(f"{short}/nbb", suffix=".csv")
    texts = [hdfs.read_text(r) for r in rels]
    if not texts:
        return {"companies": 0, "years": 0}
    return build_gold(db, {number: texts})


# ---------------------------------------------------------------------------
# CLI (jamais exécutée à l'import)
# ---------------------------------------------------------------------------
def _main(argv=None) -> int:
    import argparse

    parser = argparse.ArgumentParser(
        description="Construit la couche GOLD hotel_gold depuis des CSV de dépôts BNB."
    )
    parser.add_argument(
        "csv_dir",
        help="Dossier racine contenant un sous-dossier par entreprise "
        "({bce}/{year}.csv ou {bce}/nbb/{year}/*.csv).",
    )
    args = parser.parse_args(argv)

    from . import mongo  # import paresseux (dépend de pymongo)

    db = mongo.get_db()
    result = build_gold(db, args.csv_dir)
    total = db[config.GOLD_COLLECTION].count_documents({})
    print("GOLD construit :", result)
    print("Total documents hotel_gold :", total)
    return 0


if __name__ == "__main__":
    import sys

    sys.exit(_main())
