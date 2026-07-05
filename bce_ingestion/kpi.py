"""Calcul des KPIs financiers depuis les codes comptables BNB (données réelles).

``financials_from_codes(codes, year)`` transforme le dictionnaire ``code -> valeur``
issu d'un dépôt NBB (``sources.consult_nbb.parse_csv``) en une batterie
d'indicateurs (compte de résultat, bilan, ratios) aux clés attendues par
l'explorateur. Les postes absents du CSV (schéma abrégé) restent à ``None`` —
affichés « — » côté UI, jamais inventés.

Utilisé à la fois par le flux de scraping (``scrape_nbb``) et par le loader
d'amorçage (``scripts/seed_from_scraping``) : les documents portent ainsi leurs
KPIs quelle que soit la voie d'ingestion.
"""
from __future__ import annotations


def _num(codes: dict, *keys):
    """Somme des codes présents (float) ; None si AUCUN n'est présent.

    Distingue « absent » (schéma abrégé -> None -> « — ») de « zéro ».
    """
    vals = []
    for k in keys:
        v = codes.get(k)
        if v is None:
            continue
        try:
            vals.append(float(v))
        except (TypeError, ValueError):
            continue
    return sum(vals) if vals else None


def _pct(num, den):
    if num is None or not den:
        return None
    return round(num / den * 100, 1)


def _rat(num, den):
    if num is None or not den:
        return None
    return round(num / den, 2)


def _add(*parts):
    """Somme en propageant None seulement si TOUTES les parts sont None."""
    present = [p for p in parts if p is not None]
    return sum(present) if present else None


def financials_from_codes(codes: dict, year: int) -> dict:
    """Construit un dict de KPIs RÉELS depuis les codes d'un dépôt NBB."""
    ca = _num(codes, "70")
    cogs = _num(codes, "60")
    marge_brute = _add(ca, -cogs if cogs is not None else None)
    charges_personnel = _num(codes, "62")
    depreciation = _num(codes, "630", "631/4")
    ebit = _num(codes, "9901")
    ebitda = _add(ebit, depreciation)
    valeur_ajoutee = _num(codes, "9800")
    charges_financieres = _num(codes, "65")
    resultat_avant_impot = _num(codes, "9903")
    impots = _num(codes, "67/77", "67", "9134")
    resultat_net = _num(codes, "9904")
    caf = _add(resultat_net, depreciation)

    immobilisations = _num(codes, "20/28")
    stocks = _num(codes, "3")
    creances = _num(codes, "40/41")
    tresorerie = _num(codes, "54/58")
    actifs_circulants = _num(codes, "29/58")
    total_actif = _num(codes, "20/58")

    fonds_propres = _num(codes, "10/15")
    provisions = _num(codes, "16")
    dettes_lt = _num(codes, "17")
    dettes_ct_fin = _num(codes, "43")
    dettes_comm = _num(codes, "44")
    dettes_ct = _num(codes, "42/48")
    dettes_totales = _num(codes, "17/49")
    dettes_fin = _add(dettes_lt, dettes_ct_fin)
    dette_nette = _add(dettes_fin, -tresorerie if tresorerie is not None else None)

    return {
        "year": year,
        "ca": ca, "cogs": cogs, "marge_brute": marge_brute,
        "valeur_ajoutee": valeur_ajoutee, "charges_personnel": charges_personnel,
        "ebitda": ebitda, "depreciation": depreciation, "ebit": ebit,
        "charges_financieres": charges_financieres,
        "resultat_avant_impot": resultat_avant_impot, "impots": impots,
        "resultat_net": resultat_net, "caf": caf,
        "immobilisations": immobilisations, "stocks": stocks, "creances": creances,
        "tresorerie": tresorerie, "actifs_circulants": actifs_circulants,
        "total_actif": total_actif,
        "fonds_propres": fonds_propres, "provisions": provisions,
        "dettes_lt": dettes_lt, "dettes_ct_fin": dettes_ct_fin,
        "dettes_comm": dettes_comm, "dettes_ct": dettes_ct,
        "dettes_fin": dettes_fin, "dettes_totales": dettes_totales,
        "dette_nette": dette_nette,
        # ratios dérivés (None si inputs absents)
        "taux_marge_brute": _pct(marge_brute, ca),
        "taux_va": _pct(valeur_ajoutee, ca),
        "taux_ebitda": _pct(ebitda, ca),
        "marge_ebit": _pct(ebit, ca),
        "marge_avant_impot": _pct(resultat_avant_impot, ca),
        "marge_nette": _pct(resultat_net, ca),
        "roe": _pct(resultat_net, fonds_propres),
        "roa": _pct(resultat_net, total_actif),
        "autonomie_fin": _pct(fonds_propres, total_actif),
        "taux_endettement": _pct(dettes_totales, total_actif),
        "gearing": _pct(dettes_fin, fonds_propres),
        "current_ratio": _rat(actifs_circulants, dettes_ct),
        "quick_ratio": _rat(_add(actifs_circulants, -stocks if stocks is not None else None), dettes_ct),
        "cash_ratio": _rat(tresorerie, dettes_ct),
        "poids_treso": _pct(tresorerie, total_actif),
        "couverture_interets": _rat(ebit, charges_financieres),
        "taux_imposition": _pct(impots, resultat_avant_impot),
    }
