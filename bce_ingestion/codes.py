"""Décodeur de libellés KBO — traduit les codes bruts en descriptions FR.

Source : ``code.csv`` du dump KBO Open Data (``config.KBO_CODE_CSV``). Ce fichier
associe, pour chaque ``Category`` (``Status``, ``JuridicalForm``, ``Nace2008``,
``Nace2025``, ...) et chaque ``Code``, une ``Description`` déclinée par langue.

Colonne ``Language`` de code.csv
--------------------------------
La colonne ``Language`` de *code.csv* contient du TEXTE (``FR``, ``NL``, ``DE``,
``EN``) — et non le code numérique utilisé ailleurs (denomination.csv). On filtre
donc directement sur ``Language == 'FR'`` pour obtenir le français.

Vérification (Category='Language')
----------------------------------
Les lignes ``Category='Language'`` donnent la correspondance numérique employée
par les AUTRES fichiers KBO (denomination.csv) :

    "Language","1","FR","français"     -> le code numérique '1' = FRANÇAIS
    "Language","2","FR","néerlandais"  -> le code numérique '2' = NÉERLANDAIS

Donc, dans denomination.csv, ``Language == '1'`` désigne bien le français (et non
'2'). ``french_language_code()`` recalcule cette valeur depuis le fichier pour
rester vérifiable.

Aucun import réseau/DB ici : lecture pure du CSV via le module ``csv`` standard.
"""
from __future__ import annotations

import csv

from . import config


# ---------------------------------------------------------------------------
# Vérification de la valeur « français » (code numérique KBO)
# ---------------------------------------------------------------------------
def french_language_code(code_csv: str = config.KBO_CODE_CSV) -> str:
    """Renvoie le CODE numérique dont le libellé FR vaut « français ».

    Inspecte les lignes ``Category='Language'`` de code.csv. Vérifié sur le dump :
    renvoie ``'1'`` (et NON ``'2'``). Repli ``'1'`` si le fichier est illisible.
    """
    try:
        with open(code_csv, newline="", encoding="utf-8") as fh:
            for row in csv.DictReader(fh):
                if (
                    row.get("Category") == "Language"
                    and row.get("Language") == "FR"
                    and (row.get("Description") or "").strip().lower() == "français"
                ):
                    return (row.get("Code") or "1").strip()
    except OSError:
        pass
    return "1"


# ---------------------------------------------------------------------------
# Chargement des libellés
# ---------------------------------------------------------------------------
def load_codes(code_csv: str = config.KBO_CODE_CSV, language: str = "FR") -> dict:
    """Charge code.csv en un dict ``{(Category, Code): Description}`` pour ``language``.

    ``language`` est la valeur TEXTE de la colonne ``Language`` de code.csv
    (``'FR'`` par défaut ; ``'NL'``, ``'DE'``, ``'EN'`` possibles). Seules les
    lignes de la langue demandée sont conservées ; les descriptions vides
    deviennent ``None``.
    """
    codes: dict = {}
    with open(code_csv, newline="", encoding="utf-8") as fh:
        for row in csv.DictReader(fh):
            if row.get("Language") != language:
                continue
            category = row.get("Category")
            code = row.get("Code")
            if category is None or code is None:
                continue
            desc = (row.get("Description") or "").strip()
            codes[(category, code)] = desc or None
    return codes


# ---------------------------------------------------------------------------
# Résolution d'un libellé
# ---------------------------------------------------------------------------
def label(codes: dict, category: str, code) -> str | None:
    """Renvoie la description FR pour ``(category, code)`` ou ``None``.

    Robuste aux variantes de zéro de tête (``'1'`` ⇄ ``'001'``) fréquentes entre
    les colonnes KBO et code.csv.
    """
    if code is None:
        return None
    s = str(code).strip()
    if s == "":
        return None

    val = codes.get((category, s))
    if val is not None:
        return val

    # Variantes de padding : '1' <-> '001', '01' <-> '1', etc.
    variants = {s, s.lstrip("0") or "0", s.zfill(3)}
    for alt in variants:
        val = codes.get((category, alt))
        if val is not None:
            return val
    return None


def nace_label(codes: dict, nace_code, versions=("Nace2008", "Nace2025")) -> str | None:
    """Meilleure description FR d'un code NACE, en essayant plusieurs versions.

    Un même code NACE peut n'être présent que dans une des tables de version. On
    interroge ``versions`` dans l'ordre (par défaut ``Nace2008`` puis
    ``Nace2025``) et on renvoie la première description trouvée.
    """
    if nace_code is None:
        return None
    for version in versions:
        val = label(codes, version, nace_code)
        if val is not None:
            return val
    return None


# ---------------------------------------------------------------------------
# Démo / vérification (jamais exécutée à l'import)
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Charge et interroge code.csv (KBO).")
    parser.add_argument("--code-csv", default=config.KBO_CODE_CSV)
    parser.add_argument("--language", default="FR")
    args = parser.parse_args()

    print("Code numérique 'français' (Category=Language) :", french_language_code(args.code_csv))
    codes = load_codes(args.code_csv, args.language)
    print("Libellés chargés :", len(codes))
    print("Status/AC        :", label(codes, "Status", "AC"))
    print("JuridicalForm/416:", label(codes, "JuridicalForm", "416"))
    print("NACE 70220       :", nace_label(codes, "70220"))
