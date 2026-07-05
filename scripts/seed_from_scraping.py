#!/usr/bin/env python3
"""Amorçage de données 100 % RÉELLES par scraping (sans dump KBO).

Pour chaque numéro BCE fourni :
  1. Ouvre une session CBSO/NBB et liste les VRAIS dépôts de comptes annuels.
  2. Upsert l'entreprise dans `companies` (dénomination = nom réel lu dans le
     CSV déposé, champ « Entity name »).
  3. Télécharge chaque CSV de comptes annuels (>= --min-year) vers la couche
     Bronze LOCALE, calcule les KPIs réels depuis les codes comptables BNB et
     les stocke dans `file_state` (source='nbb').
  4. (option --ejustice) Liste les publications eJustice réelles -> `file_state`.

Aucune donnée synthétique n'est produite. Exemples :
  export BCE_MONGO_URI=mongodb://localhost:27017
  export BCE_HDFS_BACKEND=local
  export BCE_HDFS_BRONZE=/home/hdcc5629/ingestion-bronze/bronze_local
  python3 scripts/seed_from_scraping.py --bces 0878065378,0203430576
  python3 scripts/seed_from_scraping.py --bces-file real_bces.txt --min-year 2021 --ejustice
"""
from __future__ import annotations

import argparse
import os
import sys

# Rend `bce_ingestion` importable quel que soit le cwd (racine du repo).
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from bce_ingestion import config, state_db
from bce_ingestion.hdfs_io import get_hdfs
from bce_ingestion.sources import consult_nbb


def bce10(value: str) -> str:
    digits = "".join(c for c in str(value) if c.isdigit())
    return digits.zfill(10) if digits else ""


def _format_bce(bce: str) -> str:
    return f"{bce[0:4]}.{bce[4:7]}.{bce[7:10]}" if len(bce) == 10 else bce


# ---------------------------------------------------------------------------
# KPIs réels : codes comptables BNB -> champs attendus par l'explorateur
# ---------------------------------------------------------------------------
def _num(codes: dict, *keys):
    """Somme des codes présents (float) ; None si AUCUN code n'est présent.

    On distingue « absent » (schéma abrégé -> None -> affiché « — ») de « zéro ».
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
    """Construit un dict de KPIs RÉELS depuis les codes d'un dépôt NBB.

    Les champs non dérivables du CSV (schéma abrégé, code absent) restent à
    ``None`` : l'explorateur les affiche « — ». Aucune valeur inventée.
    """
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

    fin = {
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
    return fin


# ---------------------------------------------------------------------------
# Scraping NBB d'une entreprise -> companies + file_state (+ Bronze local)
# ---------------------------------------------------------------------------
def seed_company_nbb(db, hdfs, bce: str, min_year: int) -> dict:
    """Amorce une entreprise réelle via ses dépôts NBB. Renvoie un résumé."""
    session = consult_nbb.make_session(bce)
    deposits = consult_nbb.get_all_deposits(session, bce)
    csv_deposits = [d for d in deposits
                    if not d.get("migration")
                    and (_year(d) or -1) >= min_year]

    denomination = None
    done = 0
    errors = 0

    state_db.set_company_status(db, bce, "in_progress")
    for dep in csv_deposits:
        ref = dep.get("reference") or dep.get("id")
        year = _year(dep)
        rel = f"{bce}/nbb/{year}/{ref}.csv"
        try:
            text = consult_nbb.fetch_csv_text(session, dep["id"])
            path = hdfs.put_bytes(text.encode("utf-8"), rel)
            codes = consult_nbb.parse_csv(text)
            denomination = denomination or _clean(codes.get("Entity name"))
            state_db.mark_done(
                db, "nbb", bce, "csv", ref, path,
                year=year, reference=ref,
                title=f"Comptes annuels {year}",
                size=len(text.encode("utf-8")),
                financials=financials_from_codes(codes, year),
            )
            done += 1
        except Exception as exc:  # noqa: BLE001 — une erreur n'arrête pas le lot
            state_db.mark_error(db, "nbb", bce, "csv", ref, str(exc), year=year)
            errors += 1

    # dénomination de repli : nom porté par un dépôt, sinon "(sans dénomination)"
    if not denomination:
        for d in deposits:
            denomination = _clean(d.get("enterpriseName") or d.get("name"))
            if denomination:
                break
    denomination = denomination or "(sans dénomination)"

    db[config.COMPANIES_COLLECTION].update_one(
        {"_id": bce},
        {"$set": {
            "bce": bce,
            "bce_formatted": _format_bce(bce),
            "denomination": denomination,
            "status": "AC",
            "type": "2",
        }},
        upsert=True,
    )

    state_db.set_company_status(
        db, bce, "done" if errors == 0 else "in_progress",
        filings_count=len(csv_deposits))

    return {"bce": bce, "denomination": denomination,
            "deposits": len(csv_deposits), "done": done, "errors": errors}


def seed_company_ejustice(db, bce: str) -> int:
    """Liste les publications eJustice réelles -> file_state (PDF non téléchargé)."""
    from bce_ingestion.sources import ejustice
    pubs = ejustice.list_publications(bce)
    n = 0
    for p in pubs:
        numac = p.get("numac")
        if not numac:
            continue
        state_db.mark_pending(
            db, "ejustice", bce, "pdf", numac,
            year=_pub_year(p.get("date")), reference=numac,
            title=p.get("type") or "Publication Moniteur belge",
            date=p.get("date"), lien=p.get("lien"),
        )
        n += 1
    return n


def _year(deposit: dict):
    try:
        return int(deposit.get("periodEndDateYear"))
    except (TypeError, ValueError):
        return None


def _pub_year(date_str):
    try:
        return int(str(date_str)[:4])
    except (TypeError, ValueError):
        return None


def _clean(v):
    if v is None:
        return None
    s = str(v).strip()
    return s or None


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def _read_bces(args) -> list[str]:
    raw: list[str] = []
    if args.bces:
        raw.extend(args.bces.split(","))
    if args.bces_file:
        with open(args.bces_file, encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if line and not line.startswith("#"):
                    raw.append(line)
    seen, out = set(), []
    for r in raw:
        b = bce10(r)
        if len(b) == 10 and b not in seen:
            seen.add(b)
            out.append(b)
    return out


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description="Amorce des données réelles par scraping NBB/eJustice.")
    ap.add_argument("--bces", help="Numéros BCE séparés par des virgules.")
    ap.add_argument("--bces-file", help="Fichier: un BCE par ligne (# = commentaire).")
    ap.add_argument("--min-year", type=int, default=config.YEAR_CSV_MIN,
                    help="Année de clôture minimale des dépôts NBB (défaut: %(default)s).")
    ap.add_argument("--ejustice", action="store_true",
                    help="Aussi lister les publications eJustice réelles.")
    args = ap.parse_args(argv)

    bces = _read_bces(args)
    if not bces:
        ap.error("Aucun BCE valide fourni (--bces ou --bces-file).")

    from bce_ingestion import mongo
    db = mongo.get_db()
    state_db.ensure_indexes(db)
    state_db.ensure_scrape_indexes(db)
    hdfs = get_hdfs()  # backend/base pilotés par BCE_HDFS_BACKEND / BCE_HDFS_BRONZE

    print(f"[seed] {len(bces)} entreprise(s) | backend HDFS={hdfs.backend} base={hdfs.base}")
    totals = {"deposits": 0, "done": 0, "errors": 0, "ejustice": 0}
    for i, bce in enumerate(bces, 1):
        try:
            r = seed_company_nbb(db, hdfs, bce, args.min_year)
        except Exception as exc:  # noqa: BLE001
            print(f"  [{i}/{len(bces)}] {bce}  ÉCHEC listing NBB: {exc}")
            continue
        line = (f"  [{i}/{len(bces)}] {bce}  {r['denomination'][:42]:42}  "
                f"dépôts={r['deposits']} ok={r['done']} err={r['errors']}")
        if args.ejustice:
            try:
                nej = seed_company_ejustice(db, bce)
                totals["ejustice"] += nej
                line += f"  ejustice={nej}"
            except Exception as exc:  # noqa: BLE001
                line += f"  ejustice=ERR({str(exc)[:40]})"
        print(line)
        totals["deposits"] += r["deposits"]
        totals["done"] += r["done"]
        totals["errors"] += r["errors"]

    comp = db[config.COMPANIES_COLLECTION].count_documents({})
    fs = db[config.STATE_COLLECTION].count_documents({})
    print(f"[seed] terminé: {totals} | companies={comp} file_state={fs}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
