"""Jeu de données *représentatif* et déterministe pour l'explorateur.

Utilisé uniquement quand MongoDB n'est pas joignable (mode démo local). Les
valeurs sont plausibles et calées sur les vrais référentiels du pipeline
(statuts KBO, formes juridiques, NACE division 55, sources nbb/notaire/ejustice,
codes comptables BNB), mais NE proviennent PAS d'une exécution réelle.

Tout est généré à partir d'une graine fixe -> résultat stable entre deux runs.
"""
from __future__ import annotations

import hashlib
import random
from functools import lru_cache

# --- référentiels (extraits réels du code / KBO) --------------------------------

STATUS_LABELS = {"AC": "Active", "JU": "En cessation judiciaire", "ST": "Arrêtée"}

FORM_LABELS = {
    "610": "Société à responsabilité limitée (SRL/BV)",
    "014": "Société anonyme (SA/NV)",
    "706": "Entreprise individuelle",
    "612": "Société en commandite / SNC",
    "611": "Société simple",
    "416": "Société privée à responsabilité limitée (ancienne SPRL)",
}

# NACE division 55 = Hébergement (secteur hôtellerie ciblé, préfixe ^55)
HOTEL_NACE = {
    "55100": "Hôtels et hébergement similaire",
    "55201": "Hébergement touristique de court séjour",
    "55202": "Chambres d'hôtes",
    "55203": "Gîtes et meublés de vacances",
    "55300": "Terrains de camping et parcs pour caravanes",
    "55400": "Auberges de jeunesse et refuges",
    "55900": "Autres hébergements",
}

# Quelques NACE hors hôtellerie pour peupler des entreprises non ciblées
OTHER_NACE = {
    "56101": "Restauration à service complet",
    "56302": "Débits de boissons",
    "62010": "Programmation informatique",
    "47111": "Commerce de détail en magasin non spécialisé",
    "41201": "Construction générale de bâtiments résidentiels",
    "68201": "Location de biens immobiliers propres",
    "70220": "Conseil pour les affaires et la gestion",
    "49410": "Transports routiers de fret",
}

REGIONS = {
    "Flandre": ["Bruges", "Anvers", "Gand", "Ostende", "Louvain", "Knokke-Heist"],
    "Wallonie": ["Namur", "Liège", "La Roche-en-Ardenne", "Durbuy", "Spa", "Mons"],
    "Bruxelles-Capitale": ["Bruxelles", "Ixelles", "Saint-Gilles", "Uccle"],
}

HOTEL_NAMES = [
    "Hôtel des Ardennes", "Brugge Beach Hotel", "Grand Hôtel de Flandre",
    "Auberge du Lac", "Camping Haute Meuse", "Résidence Léopold",
    "Hôtel Le Cygne", "Domaine des Collines", "B&B Maison Nova",
    "Gîte du Vieux Moulin", "Hôtel Panorama", "Sea Lodge Ostende",
    "Château de Namur", "City Loft Bruxelles", "Auberge de Jeunesse Meininger",
    "Hôtel Métropole Anvers", "Les Chambres du Beffroi", "Escapade Ardennaise",
]

OTHER_NAMES = [
    "Boulangerie Delvaux", "TechnoSoft Solutions", "Ateliers Meunier",
    "Transports Rasson", "Brasserie du Centre", "Immo Horizon",
    "Conseil & Stratégie SC", "Épicerie Fine Nord",
]

SOURCES = ("nbb", "notaire", "ejustice")


def _rng(seed_text: str) -> random.Random:
    """Générateur pseudo-aléatoire déterministe dérivé d'un texte."""
    h = hashlib.sha256(seed_text.encode("utf-8")).hexdigest()
    return random.Random(int(h[:16], 16))


def format_bce(bce: str) -> str:
    """0203430576 -> 0203.430.576."""
    bce = bce.zfill(10)
    return f"{bce[0:4]}.{bce[4:7]}.{bce[7:10]}"


def _make_bce(i: int) -> str:
    # numéros plausibles commençant par 0 (personnes morales BCE)
    return str(2_000_000_000 + i * 1_234_577 % 800_000_000).zfill(10)


def _financials(rng: random.Random, year: int, scale: float) -> dict:
    """Batterie complète de KPI d'un exercice (codes comptables BNB).

    Le bilan est construit de façon (approximativement) équilibrée
    actif = passif afin que les ratios dérivés restent cohérents.
    """
    U = rng.uniform

    # --- compte de résultat ---
    ca = round(scale * U(0.85, 1.15) * (1 + (year - 2021) * 0.06), 0)   # 70
    cogs = round(ca * U(0.50, 0.62), 0)                                 # 60
    marge_brute = ca - cogs
    charges_personnel = round(ca * U(0.26, 0.40), 0)                    # 62
    autres_charges_expl = round(ca * U(0.02, 0.05), 0)
    ebitda = round(ca * U(0.14, 0.26), 0)
    valeur_ajoutee = ebitda + charges_personnel + autres_charges_expl   # 9800 approx
    depreciation = round(ca * U(0.05, 0.10), 0)                         # 630
    ebit = round(ebitda - depreciation, 0)                              # 9901

    # --- bilan : actif ---
    immobilisations = round(ca * U(0.55, 1.15), 0)                      # 20/28
    stocks = round(ca * U(0.02, 0.07), 0)                               # 3
    creances = round(ca * U(0.06, 0.13), 0)                             # 40/41
    tresorerie = round(ca * U(0.05, 0.16), 0)                           # 54/58
    autres_ac = round(ca * U(0.01, 0.04), 0)
    actifs_circulants = stocks + creances + tresorerie + autres_ac      # 29/58
    total_actif = immobilisations + actifs_circulants                   # 20/58

    # --- bilan : passif (équilibré) ---
    fonds_propres = round(total_actif * U(0.28, 0.52), 0)               # 10/15
    provisions = round(total_actif * U(0.0, 0.04), 0)                   # 16
    dettes_totales = max(0, total_actif - fonds_propres - provisions)
    dettes_fin = round(dettes_totales * U(0.4, 0.7), 0)                 # 17 + 43
    dettes_lt = round(dettes_fin * U(0.5, 0.8), 0)                      # 17
    dettes_ct_fin = max(0, dettes_fin - dettes_lt)                      # 43
    dettes_comm = round(cogs * U(0.06, 0.14), 0)                        # 44
    autres_dettes = dettes_totales - dettes_fin - dettes_comm           # plug (court terme)
    if autres_dettes < 0:
        dettes_comm = max(0, dettes_comm + autres_dettes)
        autres_dettes = max(0, dettes_totales - dettes_fin - dettes_comm)
    dettes_ct = dettes_ct_fin + dettes_comm + autres_dettes             # 42/48
    capitaux_permanents = fonds_propres + provisions + dettes_lt
    capitaux_engages = fonds_propres + dettes_fin

    # --- résultat financier & net ---
    charges_financieres = round(dettes_fin * U(0.02, 0.05), 0)          # 65
    resultat_avant_impot = round(ebit - charges_financieres, 0)         # 9903
    impots = round(max(0, resultat_avant_impot) * U(0.20, 0.25), 0)     # 67/77
    resultat_net = round(resultat_avant_impot - impots, 0)              # 9904
    caf = resultat_net + depreciation + provisions                      # capacité d'autofinancement

    # --- équilibres financiers ---
    dette_nette = dettes_fin - tresorerie
    fonds_roulement = capitaux_permanents - immobilisations             # FR
    bfr = (stocks + creances) - dettes_comm                            # besoin en FR
    tresorerie_nette = fonds_roulement - bfr
    effectif = rng.randint(3, 90)                                       # ETP estimés

    def pct(num, den):
        return round(num / den * 100, 1) if den else 0.0

    def rat(num, den):
        return round(num / den, 2) if den else 0.0

    def days(num, den):
        return round(num / den * 365, 0) if den else 0

    return {
        "year": year,
        # ===== compte de résultat =====
        "ca": ca,                                # 70
        "cogs": cogs,                            # 60
        "marge_brute": marge_brute,              # 70-60
        "valeur_ajoutee": valeur_ajoutee,        # 9800
        "charges_personnel": charges_personnel,  # 62
        "ebitda": ebitda,
        "depreciation": depreciation,            # 630
        "ebit": ebit,                            # 9901
        "charges_financieres": charges_financieres,  # 65
        "resultat_avant_impot": resultat_avant_impot,  # 9903
        "impots": impots,                        # 67/77
        "resultat_net": resultat_net,            # 9904
        "caf": caf,
        # ===== bilan actif =====
        "immobilisations": immobilisations,      # 20/28
        "stocks": stocks,                        # 3
        "creances": creances,                    # 40/41
        "tresorerie": tresorerie,                # 54/58
        "actifs_circulants": actifs_circulants,  # 29/58
        "total_actif": total_actif,              # 20/58
        # ===== bilan passif =====
        "fonds_propres": fonds_propres,          # 10/15
        "provisions": provisions,                # 16
        "dettes_lt": dettes_lt,                  # 17
        "dettes_ct_fin": dettes_ct_fin,          # 43
        "dettes_fin": dettes_fin,                # 17+43
        "dettes_comm": dettes_comm,              # 44
        "dettes_ct": dettes_ct,                  # 42/48
        "dettes_totales": dettes_totales,
        "capitaux_permanents": capitaux_permanents,
        "capitaux_engages": capitaux_engages,
        # ===== équilibres =====
        "dette_nette": dette_nette,
        "fonds_roulement": fonds_roulement,
        "bfr": bfr,
        "tresorerie_nette": tresorerie_nette,
        "effectif": effectif,
        # ===== rentabilité (marges) =====
        "taux_marge_brute": pct(marge_brute, ca),
        "taux_va": pct(valeur_ajoutee, ca),
        "taux_ebitda": pct(ebitda, ca),
        "marge_ebit": pct(ebit, ca),
        "marge_avant_impot": pct(resultat_avant_impot, ca),
        "marge_nette": pct(resultat_net, ca),
        # ===== rendement des capitaux =====
        "roe": pct(resultat_net, fonds_propres),
        "roa": pct(resultat_net, total_actif),
        "roce": pct(ebit, capitaux_engages),
        "rotation_actif": rat(ca, total_actif),
        # ===== structure & solvabilité =====
        "autonomie_fin": pct(fonds_propres, total_actif),
        "taux_endettement": pct(dettes_totales, total_actif),
        "gearing": pct(dettes_fin, fonds_propres),
        "dette_ebitda": rat(dette_nette, ebitda),
        "couverture_interets": rat(ebit, charges_financieres),
        "capacite_remboursement": rat(dettes_fin, caf),
        # ===== liquidité =====
        "current_ratio": rat(actifs_circulants, dettes_ct),
        "quick_ratio": rat(actifs_circulants - stocks, dettes_ct),
        "cash_ratio": rat(tresorerie, dettes_ct),
        "poids_treso": pct(tresorerie, total_actif),
        # ===== rotation & délais (jours) =====
        "dso": days(creances, ca),               # délai clients
        "dpo": days(dettes_comm, cogs),          # délai fournisseurs
        "dio": days(stocks, cogs),               # stocks en jours
        "rotation_stocks": rat(cogs, stocks),
        # ===== productivité & personnel =====
        "ca_par_etp": round(ca / effectif, 0) if effectif else 0,
        "va_par_etp": round(valeur_ajoutee / effectif, 0) if effectif else 0,
        "resultat_par_etp": round(resultat_net / effectif, 0) if effectif else 0,
        "charges_perso_sur_ca": pct(charges_personnel, ca),
        "partage_va": pct(charges_personnel, valeur_ajoutee),
        "taux_imposition": pct(impots, resultat_avant_impot),
    }


def _documents(rng: random.Random, bce: str, is_hotel: bool) -> list[dict]:
    """Documents Bronze liés à une entreprise (file_state + chemin HDFS)."""
    docs: list[dict] = []

    # --- NBB / CBSO : comptes annuels (surtout pour les hôtels ciblés) ---
    if is_hotel or rng.random() < 0.3:
        scale = rng.choice([650_000, 900_000, 1_400_000, 2_300_000, 4_100_000])
        years = [y for y in (2021, 2022, 2023, 2024) if rng.random() > 0.15]
        if not years:
            years = [2023, 2024]
        for y in years:
            ref = f"REF-{y}-{rng.randint(1000, 9999)}"
            docs.append({
                "source": "nbb", "kind": "csv", "ref": ref, "year": y,
                "status": "done",
                "hdfs_path": f"hdfs:///bronze/{bce}/nbb/{y}/{ref}.csv",
                "size": rng.randint(4_000, 22_000),
                "title": f"Comptes annuels {y}",
                "financials": _financials(rng, y, scale),
            })
        # un dépôt 2025 partiel, en cours
        if rng.random() < 0.4:
            ref = f"REF-2025-{rng.randint(1000, 9999)}"
            docs.append({
                "source": "nbb", "kind": "csv", "ref": ref, "year": 2025,
                "status": "in_progress",
                "hdfs_path": "—",
                "size": 0,
                "title": "Comptes annuels 2025 (dépôt partiel)",
                "financials": None,
            })

    # --- eJustice / Moniteur belge : publications ---
    for _ in range(rng.randint(1, 4)):
        y = rng.randint(2019, 2025)
        numac = str(rng.randint(19_000_000, 25_999_999))
        types = ["Constitution", "Modification des statuts", "Nomination",
                 "Démission", "Bilan / comptes annuels", "Transfert de siège"]
        st = "done" if rng.random() > 0.08 else "error"
        docs.append({
            "source": "ejustice", "kind": "pdf", "ref": numac, "year": y,
            "status": st,
            "hdfs_path": f"hdfs:///bronze/{bce}/ejustice/{numac}.pdf" if st == "done" else "—",
            "size": rng.randint(30_000, 400_000) if st == "done" else 0,
            "title": rng.choice(types),
            "date": f"{y}-{rng.randint(1,12):02d}-{rng.randint(1,28):02d}",
        })

    # --- Notaire.be : statuts / actes constitutifs ---
    for _ in range(rng.randint(0, 2)):
        y = rng.randint(2018, 2024)
        doc_id = _rng(bce + str(y)).getrandbits(28)
        deed = f"{y}-{rng.randint(1,12):02d}-{rng.randint(1,28):02d}"
        docs.append({
            "source": "notaire", "kind": "pdf", "ref": f"{doc_id:07x}", "year": y,
            "status": "done",
            "hdfs_path": f"hdfs:///bronze/{bce}/notaire/{deed}_{doc_id:07x}.pdf",
            "size": rng.randint(80_000, 600_000),
            "title": "Statuts (acte constitutif)" if rng.random() > 0.5 else "Statuts coordonnés",
            "date": deed,
        })

    return docs


def _build_company(i: int) -> dict:
    bce = _make_bce(i)
    rng = _rng(bce)
    is_hotel = i % 3 != 0  # ~2/3 d'hôtels (secteur ciblé)

    if is_hotel:
        name = HOTEL_NAMES[i % len(HOTEL_NAMES)]
        nace_code = rng.choice(list(HOTEL_NACE))
        nace_label = HOTEL_NACE[nace_code]
    else:
        name = OTHER_NAMES[i % len(OTHER_NAMES)]
        nace_code = rng.choice(list(OTHER_NACE))
        nace_label = OTHER_NACE[nace_code]

    suffix = rng.choice(["SRL", "SA", "BV", "SC", ""])
    denomination = f"{name} {suffix}".strip().upper()

    region = rng.choice(list(REGIONS))
    city = rng.choice(REGIONS[region])
    form = rng.choice(list(FORM_LABELS))
    status = "AC" if rng.random() > 0.1 else rng.choice(["JU", "ST"])

    docs = _documents(rng, bce, is_hotel)
    counts = {s: sum(1 for d in docs if d["source"] == s) for s in SOURCES}
    counts["total"] = len(docs)

    return {
        "bce": bce,
        "bce_formatted": format_bce(bce),
        "denomination": denomination,
        "status": status,
        "status_label": STATUS_LABELS.get(status, status),
        "type": "2",  # personne morale
        "juridical_form": form,
        "form_label": FORM_LABELS.get(form, form),
        "nace_code": nace_code,
        "nace_label": nace_label,
        "is_hospitality": is_hotel,
        "region": region,
        "city": city,
        "start_date": f"{rng.randint(1,28):02d}-{rng.randint(1,12):02d}-{rng.randint(1975, 2020)}",
        "doc_counts": counts,
        "documents": docs,
    }


@lru_cache(maxsize=1)
def dataset() -> dict:
    """Renvoie {bce: company} pour l'ensemble du jeu représentatif."""
    companies = [_build_company(i) for i in range(64)]
    return {c["bce"]: c for c in companies}


if __name__ == "__main__":  # aperçu rapide
    d = dataset()
    print(f"{len(d)} entreprises représentatives générées.")
    sample = next(iter(d.values()))
    print(sample["denomination"], sample["bce_formatted"],
          "| docs:", sample["doc_counts"])
