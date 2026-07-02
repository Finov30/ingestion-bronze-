#!/usr/bin/env bash
# Lance le backend FastAPI en mode dev (rechargement auto).
#
# Le backend réutilise le package `bce_ingestion` (config + scraper notaire).
# Il ajoute automatiquement son dossier parent au sys.path (voir app/db.py),
# mais vous pouvez aussi forcer via PYTHONPATH / BCE_PACKAGE_PARENT.
set -euo pipefail
cd "$(dirname "$0")"

# Emplacement du package bce_ingestion (dossier PARENT).
export BCE_PACKAGE_PARENT="${BCE_PACKAGE_PARENT:-/mnt/c/Users/HDCC5629/Downloads}"

# Connexion Mongo (surchargeable).
export BCE_MONGO_URI="${BCE_MONGO_URI:-mongodb://localhost:27017}"
export BCE_MONGO_DB="${BCE_MONGO_DB:-bce}"

# Proxies Tor : DÉSACTIVÉS par défaut. Décommentez pour activer :
# export BCE_TOR_PROXIES="socks5h://127.0.0.1:9050"

exec uvicorn app.main:app --reload --host 0.0.0.0 --port 8000
