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
