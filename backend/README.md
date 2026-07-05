# BCE Consultation — Backend FastAPI (Jour 3)

Backend REST + SSE qui expose la couche **Silver** (identité d'entreprise) et la
couche **Gold** (indicateurs financiers NBB + ratios), et gratte à la demande les
**dirigeants** (kbopub) et les **statuts notariés** (notaire.be, en streaming).

Il réutilise le package `bce_ingestion` (Jour 1/2) pour :
- les noms de collections Mongo (`bce_ingestion.config`),
- le scraper notaire.be (`bce_ingestion.sources.strapor_notaire`).

## Structure

```
backend/
  app/
    main.py              # app FastAPI + CORS + /api/health + include routers
    db.py                # accès Mongo (pymongo lazy, injectable pour tests)
    scrapers.py          # scraper dirigeants kbopub (requests + bs4)
    routers/
      search.py          # GET /api/search
      enterprise.py      # GET /api/enterprise/{bce}
      dirigeants.py      # GET /api/enterprise/{bce}/dirigeants
      statutes.py        # GET /api/enterprise/{bce}/statutes/stream (SSE)
  requirements.txt
  run.sh
  README.md
```

## Installation

```bash
pip install -r requirements.txt        # fastapi absent par défaut -> installe
```

Le backend doit pouvoir importer `bce_ingestion`. Deux options :

1. **Automatique** (par défaut) : `app/db.py` ajoute le dossier *parent* du
   package (`/mnt/c/Users/HDCC5629/Downloads`) à `sys.path`. Surchargeable via
   la variable d'environnement `BCE_PACKAGE_PARENT`.
2. **PYTHONPATH** : `export PYTHONPATH=/mnt/c/Users/HDCC5629/Downloads`.

## Lancement

```bash
./run.sh
# = uvicorn app.main:app --reload --host 0.0.0.0 --port 8000
```

## Configuration (variables d'environnement)

| Variable              | Défaut                        | Rôle                                   |
|-----------------------|-------------------------------|----------------------------------------|
| `BCE_MONGO_URI`       | `mongodb://localhost:27017`   | URI MongoDB                            |
| `BCE_MONGO_DB`        | `bce`                         | Base MongoDB                          |
| `BCE_PACKAGE_PARENT`  | `/mnt/c/Users/HDCC5629/Downloads` | Dossier parent de `bce_ingestion` |
| `BCE_TOR_PROXIES`     | *(vide → OFF)*                | Proxies Tor pour le scraping (ex. `socks5h://127.0.0.1:9050`) |

Les noms de collections proviennent de `bce_ingestion.config` :
`enterprise_silver` (Silver), `hotel_gold` (Gold). Les caches de scraping sont
les collections `dirigeants` et `statutes` (clé `_id` = numéro BCE 10 chiffres).

### Proxies Tor

**Désactivés par défaut** (aucun proxy). Pour les activer, renseigner
`BCE_TOR_PROXIES`. Utile pour contourner un rate-limit lors du scraping kbopub.

## Endpoints (contrat API)

### `GET /api/health`
```json
{"status": "ok"}
```

### `GET /api/search?q=<nom|bce>&limit=20`
- `q` uniquement des chiffres → match par **préfixe** du numéro (`_id`).
- sinon → **regex insensible à la casse** sur `denomination_principale` et
  `denominations`.
```json
{"results": [{"enterprise_number", "denomination", "status_label", "juridical_form_label"}]}
```

### `GET /api/enterprise/{bce}`
```json
{
  "enterprise_number": "0878065378",
  "silver": {"denomination_principale", "StatusLabel", "JuridicalFormLabel", "StartDate",
             "address": {"StreetFR","HouseNumber","Zipcode","MunicipalityFR"} | null,
             "activities": [{"NaceCode","Classification","NaceLabel"}]} | null,
  "gold":   {"schema_type","last_updated",
             "years": [{"year","ca","marge_brute","ebit","resultat_net","tresorerie",
                        "dettes_financieres","fonds_propres","capital_souscrit",
                        "ratios": {"marge_nette","roe","ratio_liquidite","taux_endettement"}}]} | null
}
```
Entreprise inconnue → `silver` et `gold` à `null` (HTTP 200, pas d'erreur).

### `GET /api/enterprise/{bce}/dirigeants`
```json
{"dirigeants": [{"nom": "...", "qualites": ["..."]}], "cached": true|false}
```
Sert depuis le cache Mongo `dirigeants` si présent ; sinon gratte kbopub
(dans un thread), persiste, renvoie. En cas d'échec réseau : `dirigeants: []`.

### `GET /api/enterprise/{bce}/statutes/stream` (SSE, `text/event-stream`)
Une ligne par statut :
```
data: {"document","date","notaire","statut","documentId"}

event: done
data: {"count": N}
```
Sert depuis le cache Mongo `statutes` si présent (stream instantané) ; sinon
gratte notaire.be via `bce_ingestion.sources.strapor_notaire` dans un thread,
streame chaque statut au fil de l'eau, puis persiste. En cas d'échec :
```
event: error
data: {"message": "...", "enterprise_number": "..."}
```
(fermeture propre, jamais de 500).

## CORS

Autorise `http://localhost:5173` (Vite dev) et `*`.

## Tests

`TestClient` (fastapi) + `mongomock` — la dépendance `get_db` est écrasée par une
base factice pré-remplie (1 doc `enterprise_silver` + 1 doc `hotel_gold` pour
`0878065378`, + caches `dirigeants` / `statutes`). Vérifie : health, search
(nom + préfixe numéro), enterprise silver+gold, entreprise inconnue,
dirigeants (cache), SSE (cache + done). Voir la suite de validation qui couvre
aussi le scrape live (mocké), la persistance et le chemin d'erreur SSE.

```bash
python3 test_backend.py     # 12/12 checks
```
