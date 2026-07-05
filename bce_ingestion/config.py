"""Configuration centralisée du pipeline d'ingestion BCE (medallion / Bronze).

Toutes les valeurs sont surchargeables par variables d'environnement, de sorte
que le même code tourne en local (tests) et sur le cluster (Airflow + MongoDB +
HDFS) sans modification.
"""
import os

# ---------------------------------------------------------------------------
# MongoDB
# ---------------------------------------------------------------------------
MONGO_URI = os.environ.get("BCE_MONGO_URI", "mongodb://localhost:27017")
MONGO_DB  = os.environ.get("BCE_MONGO_DB", "bce")

COMPANIES_COLLECTION = os.environ.get("BCE_COMPANIES_COLLECTION", "companies")
STATE_COLLECTION     = os.environ.get("BCE_STATE_COLLECTION", "file_state")

# --- Jour 2 : couche Silver + ciblage hôtellerie ---
# Bronze « riche » (entreprises + activités/adresses/dénominations imbriquées).
FINALE_COLLECTION = os.environ.get("BCE_FINALE_COLLECTION", "enterprise_finale")
# Silver (documents nettoyés/enrichis).
SILVER_COLLECTION = os.environ.get("BCE_SILVER_COLLECTION", "enterprise_silver")
# State DB au niveau ENTREPRISE (statut de scraping : pending/in_progress/done).
SCRAPE_STATE_COLLECTION = os.environ.get("BCE_SCRAPE_STATE_COLLECTION", "scrape_state")

# --- Jour 3 : couche Gold (agrégats financiers annuels par entreprise) ---
GOLD_COLLECTION = os.environ.get("BCE_GOLD_COLLECTION", "hotel_gold")

# ---------------------------------------------------------------------------
# HDFS — couche Bronze (données brutes)
# ---------------------------------------------------------------------------
# Racine de la couche Bronze sur HDFS.
HDFS_BRONZE = os.environ.get("BCE_HDFS_BRONZE", "hdfs:///bronze")
# Backend d'écriture HDFS : "pyarrow" (HadoopFileSystem), "cli" (hdfs dfs) ou
# "local" (écrit dans un dossier local — pour les tests hors cluster).
HDFS_BACKEND = os.environ.get("BCE_HDFS_BACKEND", "pyarrow")

# ---------------------------------------------------------------------------
# Source KBO Open Data (pour peupler MongoDB)
# ---------------------------------------------------------------------------
KBO_ENTERPRISE_CSV   = os.environ.get("BCE_KBO_ENTERPRISE_CSV",   "/data/kbo/enterprise.csv")
KBO_DENOMINATION_CSV = os.environ.get("BCE_KBO_DENOMINATION_CSV", "/data/kbo/denomination.csv")
KBO_ADDRESS_CSV      = os.environ.get("BCE_KBO_ADDRESS_CSV",      "/data/kbo/address.csv")
KBO_ACTIVITY_CSV     = os.environ.get("BCE_KBO_ACTIVITY_CSV",     "/data/kbo/activity.csv")
KBO_CONTACT_CSV      = os.environ.get("BCE_KBO_CONTACT_CSV",      "/data/kbo/contact.csv")
KBO_ESTABLISHMENT_CSV = os.environ.get("BCE_KBO_ESTABLISHMENT_CSV", "/data/kbo/establishment.csv")
KBO_BRANCH_CSV       = os.environ.get("BCE_KBO_BRANCH_CSV",       "/data/kbo/branch.csv")
KBO_CODE_CSV         = os.environ.get("BCE_KBO_CODE_CSV",         "/data/kbo/code.csv")

# ---------------------------------------------------------------------------
# Débit / politesse (les portails publics limitent le débit)
# ---------------------------------------------------------------------------
NBB_DELAY      = float(os.environ.get("BCE_NBB_DELAY", "0.6"))
NOTAIRE_DELAY  = float(os.environ.get("BCE_NOTAIRE_DELAY", "0.3"))
EJUSTICE_DELAY = float(os.environ.get("BCE_EJUSTICE_DELAY", "0.4"))
HTTP_RETRIES   = int(os.environ.get("BCE_HTTP_RETRIES", "5"))

# Playwright (renouvellement des cookies F5 de notaire.be) : headless sur cluster.
NOTAIRE_HEADLESS = os.environ.get("BCE_NOTAIRE_HEADLESS", "1") == "1"

# ---------------------------------------------------------------------------
# Ordonnancement
# ---------------------------------------------------------------------------
# Nombre d'entreprises traitées par exécution de DAG (le pipeline balaie
# l'ensemble des ~1,95 M d'entreprises au fil des runs).
BATCH_SIZE = int(os.environ.get("BCE_BATCH_SIZE", "200"))

# Années minimales à récupérer (comme dans le notebook / les fichiers du prof).
YEAR_PDF_MIN = int(os.environ.get("BCE_YEAR_PDF_MIN", "2000"))
YEAR_CSV_MIN = int(os.environ.get("BCE_YEAR_CSV_MIN", "2021"))

# ---------------------------------------------------------------------------
# Ciblage sectoriel — HÔTELLERIE / HÉBERGEMENT (Jour 2)
# ---------------------------------------------------------------------------
# Préfixes de codes NACE (toutes versions 2003/2008/2025) qui définissent le
# secteur « hôtellerie / hébergement ». La division NACE 55 = « Hébergement »
# (55.10 Hôtels, 55.20 hébergement touristique, 55.90 autres hébergements…).
# Surchargeable (ex. "55,56" pour inclure la restauration → HORECA complet).
HOTEL_NACE_PREFIXES = [
    p.strip()
    for p in os.environ.get("BCE_HOTEL_NACE_PREFIXES", "55").split(",")
    if p.strip()
]
