"""Référentiels de libellés (KBO / NACE / codes comptables) — données RÉELLES.

Ces tables ne sont PAS des fiches d'entreprises : ce sont des dictionnaires de
correspondance code -> libellé, extraits des référentiels officiels (statuts KBO,
formes juridiques, division NACE 55 « Hébergement »). Elles servent à afficher un
libellé lisible à partir d'un code stocké en base, en mode 100 % réel.

Aucun contenu synthétique ici : la génération de fausses entreprises (ancien
`sample_data.py`) a été supprimée — l'explorateur ne sert QUE des données Mongo.
"""
from __future__ import annotations

# Statuts d'entreprise KBO.
STATUS_LABELS = {"AC": "Active", "JU": "En cessation judiciaire", "ST": "Arrêtée"}

# Formes juridiques KBO (codes -> libellé).
FORM_LABELS = {
    "610": "Société à responsabilité limitée (SRL/BV)",
    "014": "Société anonyme (SA/NV)",
    "706": "Entreprise individuelle",
    "612": "Société en commandite / SNC",
    "611": "Société simple",
    "416": "Société privée à responsabilité limitée (ancienne SPRL)",
}

# NACE division 55 = Hébergement (secteur hôtellerie ciblé, préfixe ^55).
HOTEL_NACE = {
    "55100": "Hôtels et hébergement similaire",
    "55201": "Hébergement touristique de court séjour",
    "55202": "Chambres d'hôtes",
    "55203": "Gîtes et meublés de vacances",
    "55300": "Terrains de camping et parcs pour caravanes",
    "55400": "Auberges de jeunesse et refuges",
    "55900": "Autres hébergements",
}

# Quelques NACE hors hôtellerie, pour libeller les entreprises non ciblées.
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

# Sources de documents Bronze.
SOURCES = ("nbb", "notaire", "ejustice")


def format_bce(bce: str) -> str:
    """0203430576 -> 0203.430.576 (regroupement 4-3-3 comme la BCE)."""
    bce = (bce or "").zfill(10)
    return f"{bce[0:4]}.{bce[4:7]}.{bce[7:10]}"
