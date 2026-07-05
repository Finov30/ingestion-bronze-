"""Agrégats du dashboard BCE — calculés en direct depuis MongoDB (100 % réel).

Une seule fonction publique, :func:`payload`, renvoie tous les blocs du dashboard
sous la forme attendue par les fonctions de rendu de ``bce-dashboard.html``. Tout
est dérivé des collections réelles :

  companies / enterprise_silver / enterprise_finale : registre KBO (si chargé)
  file_state       : documents Bronze ingérés (NBB/eJustice/notaire)
  scrape_state     : avancement du scraping par entreprise

Les blocs qui dépendent du registre KBO (funnel hôtellerie, NACE, régions,
formes juridiques) sont vides tant que le dump KBO n'est pas chargé — jamais
remplis de valeurs fictives.
"""
from __future__ import annotations

from .. import config
from . import labels

# Couleurs (alignées sur les variables CSS du dashboard).
SRC_META = {
    "nbb": ("NBB / CBSO", "var(--nbb)"),
    "ejustice": ("eJustice", "var(--ejustice)"),
    "notaire": ("Notaire", "var(--notaire)"),
}
REGION_COLORS = {
    "Flandre": "#8A621B",
    "Wallonie": "#B0812C",
    "Bruxelles-Capitale": "#D8B563",
}
SCRAPE_META = [
    ("done", "var(--done)"), ("pending", "var(--pending)"),
    ("in_progress", "var(--progress)"),
]

# Sources réellement ingérées (seul le NBB est scrapé pour l'instant) : on
# n'affiche PAS notaire/eJustice tant qu'aucun document n'en provient.
ACTIVE_SOURCES = ("nbb",)

# Requête d'éligibilité hôtellerie (identique à hotel.find_hotels).
def _hotel_query() -> dict:
    from ..hotel import EXCLUDED_FORMS
    prefixes = config.HOTEL_NACE_PREFIXES or ["55"]
    return {
        "Status": "AC",
        "TypeOfEnterprise": "2",
        "JuridicalForm": {"$nin": list(EXCLUDED_FORMS)},
        "activities": {"$elemMatch": {
            "Classification": "MAIN",
            "NaceCode": {"$regex": "^(" + "|".join(prefixes) + ")"},
        }},
    }

# Requête « hébergement » plus large (NACE ^55, sans les autres critères).
HOSPITALITY_ANY = {"activities": {"$elemMatch": {"NaceCode": {"$regex": "^55"}}}}


def _region_from_zip(zipcode) -> str:
    try:
        z = int(str(zipcode)[:4])
    except (TypeError, ValueError):
        return ""
    if 1000 <= z <= 1299:
        return "Bruxelles-Capitale"
    if 1300 <= z <= 1499 or 4000 <= z <= 7999:
        return "Wallonie"
    if 1500 <= z <= 3999 or 8000 <= z <= 9999:
        return "Flandre"
    return ""


def _count(coll, q=None):
    return coll.count_documents(q or {})


def payload(db) -> dict:
    companies = db[config.COMPANIES_COLLECTION]
    silver = db[config.SILVER_COLLECTION]
    finale = db[config.FINALE_COLLECTION]
    file_state = db[config.STATE_COLLECTION]
    scrape_state = db[config.SCRAPE_STATE_COLLECTION]

    # Collection « registre » de référence : silver si peuplée, sinon finale.
    registry = silver if _count(silver) else finale
    has_registry = _count(registry) > 0

    # -- compteurs de base --
    n_companies = _count(companies) or _count(silver)
    n_silver = _count(silver)
    docs_done = _count(file_state, {"status": "done"})
    nbb_done = _count(file_state, {"source": "nbb", "kind": "csv", "status": "done"})
    fs_total = _count(file_state)
    fs_error = _count(file_state, {"status": "error"})
    err_rate = round(fs_error / fs_total * 100, 1) if fs_total else 0.0

    hotels_eligible = _count(registry, _hotel_query()) if has_registry else 0
    hotels_any = _count(registry, HOSPITALITY_ANY) if has_registry else 0
    scr = {"done": 0, "pending": 0, "in_progress": 0}
    for row in scrape_state.aggregate([{"$group": {"_id": "$status", "n": {"$sum": 1}}}]):
        if row["_id"] in scr:
            scr[row["_id"]] = row["n"]

    # ------------------------------------------------------------------ KPI
    kpi = [
        {"label": "Entreprises référencées", "val": n_companies,
         "unit": "companies (seed)", "accent": "var(--gold)"},
        {"label": "Documents Bronze ingérés", "val": docs_done,
         "unit": "fichiers PDF/CSV (statut done)", "accent": "var(--nbb)"},
        {"label": "Hôtels ciblés (NACE 55)", "val": hotels_eligible,
         "unit": "établissements <b>is_hospitality</b>", "accent": "var(--gold)"},
        {"label": "Hôtels scrapés (done)", "val": scr["done"],
         "unit": (f"<b>{round(scr['done'] / hotels_eligible * 100, 1)} %</b> du ciblage"
                  if hotels_eligible else "entreprises scrapées"),
         "accent": "var(--done)"},
        {"label": "Dépôts NBB ≥ 2021", "val": nbb_done,
         "unit": "comptes annuels CSV", "accent": "var(--nbb)"},
        {"label": "Taux d'erreur ingestion", "val": f"{err_rate} %".replace(".", ","),
         "raw": True, "unit": "fichiers en statut <b>error</b>", "accent": "var(--error)"},
    ]

    # --------------------------------------------------------------- funnels
    funnel_a = [
        {"name": "Seed companies (KBO)", "val": n_companies, "color": "#6C7686"},
        {"name": "Actives (Status = AC)",
         "val": _count(registry, {"Status": "AC"}) if has_registry
                else _count(companies, {"status": "AC"}), "color": "#586173"},
        {"name": "Silver enrichi", "val": n_silver, "color": "#8B94A3"},
    ]
    funnel_b = [
        {"name": "Hébergement (NACE ^55)", "val": hotels_any, "color": "var(--gold)"},
        {"name": "Éligibles (AC·type 2·forme·MAIN 55)", "val": hotels_eligible,
         "color": "var(--gold-deep)"},
        {"name": "Chargées « pending »", "val": scr["pending"] + scr["in_progress"] + scr["done"],
         "color": "#B98A3C"},
        {"name": "Scrapées « done »", "val": scr["done"], "color": "var(--done)"},
    ]

    # --------------------------------------------------------------- sources
    by_source = {}
    for row in file_state.aggregate([
        {"$match": {"status": "done"}},
        {"$group": {"_id": "$source", "n": {"$sum": 1}}},
    ]):
        by_source[row["_id"]] = row["n"]
    sources = [{"label": SRC_META[s][0], "key": s, "val": by_source.get(s, 0),
                "color": SRC_META[s][1]} for s in ACTIVE_SOURCES]

    # ---------------------------------------------------------------- status
    by_ss = {}
    for row in file_state.aggregate([
        {"$group": {"_id": {"source": "$source", "status": "$status"}, "n": {"$sum": 1}}},
    ]):
        src = row["_id"].get("source")
        st = row["_id"].get("status")
        by_ss.setdefault(src, {}).setdefault(st, 0)
        by_ss[src][st] = row["n"]
    status = [{"src": s, "color": SRC_META[s][1],
               "done": by_ss.get(s, {}).get("done", 0),
               "pending": by_ss.get(s, {}).get("pending", 0),
               "error": by_ss.get(s, {}).get("error", 0)}
              for s in ACTIVE_SOURCES]

    # ---------------------------------------------------------------- scrape
    scrape = [{"label": lbl, "val": scr.get(lbl, 0), "color": col}
              for (lbl, col) in SCRAPE_META]

    # ------------------------------------------------------------------ nace
    nace = []
    if has_registry:
        for row in registry.aggregate([
            {"$match": HOSPITALITY_ANY},
            {"$unwind": "$activities"},
            {"$match": {"activities.Classification": "MAIN",
                        "activities.NaceCode": {"$regex": "^55"}}},
            {"$group": {"_id": "$activities.NaceCode", "n": {"$sum": 1},
                        "lbl": {"$first": "$activities.NaceLabel"}}},
            {"$sort": {"n": -1}},
        ]):
            code = row["_id"]
            # Libellé réel KBO (code.csv) porté par le Silver ; repli sur le dico.
            lbl = row.get("lbl") or labels.HOTEL_NACE.get(code, "Hébergement " + str(code))
            nace.append({"code": code, "lbl": lbl, "val": row["n"]})

    # --------------------------------------------------------------- regions
    regions = []
    if has_registry:
        buckets = {"Flandre": 0, "Wallonie": 0, "Bruxelles-Capitale": 0}
        for doc in registry.find(_hotel_query(), {"address": 1, "seat": 1, "region": 1}):
            addr = doc.get("address") or doc.get("seat") or {}
            reg = doc.get("region") or _region_from_zip(
                addr.get("Zipcode") or addr.get("zipcode") or "")
            if reg in buckets:
                buckets[reg] += 1
        tot = sum(buckets.values())
        for name, val in buckets.items():
            regions.append({"name": "Bruxelles-Cap." if name.startswith("Brux") else name,
                            "val": val, "pct": round(val / tot * 100) if tot else 0,
                            "color": REGION_COLORS[name]})

    # ------------------------------------------------------------------ years
    years = []
    for row in file_state.aggregate([
        {"$match": {"source": "nbb", "kind": "csv", "status": "done"}},
        {"$group": {"_id": "$year", "n": {"$sum": 1}}},
        {"$sort": {"_id": 1}},
    ]):
        if row["_id"]:
            years.append({"y": str(row["_id"]), "val": row["n"]})

    # ------------------------------------------------------------------ forms
    forms = []
    if has_registry:
        for row in registry.aggregate([
            {"$match": _hotel_query()},
            {"$group": {"_id": "$JuridicalForm", "n": {"$sum": 1},
                        "lbl": {"$first": "$JuridicalFormLabel"}}},
            {"$sort": {"n": -1}}, {"$limit": 8},
        ]):
            code = row["_id"] or "—"
            # Libellé réel KBO (code.csv) porté par le Silver ; repli sur le dico.
            lbl = row.get("lbl") or labels.FORM_LABELS.get(code, "Autre forme")
            forms.append({"code": code, "lbl": lbl, "val": row["n"]})

    # --------------------------------------------------------------- deposits
    deposits = []
    denom_cache = {}
    for s in file_state.find({"source": "nbb"}).sort("updated_at", -1).limit(5):
        bce = s.get("bce", "")
        if bce not in denom_cache:
            c = (companies.find_one({"_id": bce}, {"denomination": 1})
                 or registry.find_one({"_id": bce}, {"denomination_principale": 1}) or {})
            denom_cache[bce] = (c.get("denomination")
                                or c.get("denomination_principale") or bce)
        deposits.append([
            bce, denom_cache[bce], str(s.get("year", "") or ""),
            str(s.get("ref", "")), s.get("source", "nbb"),
            s.get("hdfs_path", "—"), s.get("status", ""),
        ])

    return {
        "kpi": kpi, "funnel_a": funnel_a, "funnel_b": funnel_b,
        "sources": sources, "status": status, "scrape": scrape,
        "nace": nace, "regions": regions, "years": years,
        "forms": forms, "deposits": deposits,
        "has_registry": has_registry,
    }
