# bce_ingestion — Pipeline d'ingestion (couche Bronze) pour données d'entreprises belges

Pipeline d'ingestion *data-engineering* qui télécharge, de façon **idempotente**,
les documents publics (CSV comptables + PDF) de ~1,95 M d'entreprises belges
(BCE/KBO) depuis **3 sources publiques**, et les dépose tels quels dans une
couche **Bronze** sur HDFS.

```
MongoDB (companies)  ->  State DB (file_state)  ->  Airflow DAGs  ->  Bronze HDFS
```

---

## 1. Architecture (medallion / couche Bronze)

Ce paquet implémente **uniquement la couche Bronze** de l'architecture medallion
(Bronze → Silver → Gold) :

- **Bronze** = données *brutes*, non transformées, telles que servies par les
  portails publics. On ne parse pas, on ne nettoie pas, on ne fusionne pas : on
  archive l'octet source, horodaté et traçable. C'est le socle qui permet de
  rejouer/retraiter toutes les couches supérieures sans re-télécharger.
- **Silver / Gold** (hors périmètre de ce paquet) : normalisation, calcul des
  KPI, agrégats analytiques.

### Diagramme de composants

```
                 KBO Open Data (enterprise.csv)
                          │  seed_companies.py
                          ▼
                ┌───────────────────────┐
                │      MongoDB           │
                │  collection companies  │   (référentiel des entreprises)
                └───────────┬───────────┘
                            │  itère par lots (BATCH_SIZE)
                            ▼
        ┌───────────────────────────────────────────────┐
        │                 Airflow DAGs                   │
        │   (un DAG / source, planifiés indépendamment)  │
        │                                                │
        │   ┌──────────┐  ┌──────────┐  ┌─────────────┐  │
        │   │  NBB     │  │ notaire  │  │  eJustice   │  │
        │   │  /CBSO   │  │  (F5)    │  │             │  │
        │   └────┬─────┘  └────┬─────┘  └──────┬──────┘  │
        └────────┼─────────────┼───────────────┼─────────┘
                 │             │               │
         is_done?│     is_done?│       is_done?│   (skip si déjà 'done')
                 ▼             ▼               ▼
        ┌───────────────────────────────────────────────┐
        │            State DB — file_state               │
        │  clé unique (source, bce, kind, ref)           │
        │  statut pending -> done | error                │
        └───────────────────────────────────────────────┘
                 │             │               │
                 ▼             ▼               ▼
        ┌───────────────────────────────────────────────┐
        │                 Bronze HDFS                    │
        │   nbb/… , notaire/… , ejustice/…               │
        └───────────────────────────────────────────────┘
```

### Les 3 sources

| Source (`source`) | Portail | Contenu | `kind` |
|-------------------|---------|---------|--------|
| `nbb`      | NBB / CBSO (Banque Nationale de Belgique) | Comptes annuels : CSV + PDF des dépôts | `csv`, `pdf` |
| `notaire`  | notaire.be (statuts, derrière un WAF F5) | Statuts non certifiés (PDF) | `pdf` |
| `ejustice` | eJustice / Moniteur belge | Publications / actes (PDF) | `pdf` |

---

## 2. Idempotence & détection de delta (State DB)

Le pipeline balaie continuellement l'ensemble des entreprises. Pour ne **jamais
re-télécharger** deux fois le même document et pour reprendre proprement après
une panne, chaque fichier est suivi dans la collection MongoDB **`file_state`**.

### Clé d'unicité

```
(source, bce, kind, ref)
```

- `source` ∈ `{nbb, notaire, ejustice}`
- `bce`    = numéro BCE 10 chiffres **avec** le zéro de tête (ex. `0203430576`)
- `kind`   ∈ `{csv, pdf}`
- `ref`    = identifiant unique du fichier au sein de `(source, bce, kind)`
             (ex. `id` de dépôt NBB, `documentId` notaire, `numac` eJustice)

Un **index unique** sur ces 4 champs garantit qu'un même document n'est archivé
qu'une seule fois (protection anti-doublon, y compris en cas d'exécutions
concurrentes de DAG).

### Cycle de vie d'un enregistrement

```
        mark_pending          mark_done
   (aucun)  ───────────►  pending  ───────────►  done   ← is_done() renvoie True
                             │
                             │ mark_error (échec réseau, HTTP, parsing…)
                             ▼
                           error   (attempts incrémenté ; rejoué au prochain run)
```

- **`is_done(db, source, bce, kind, ref)`** est appelé **avant** tout
  téléchargement : s'il renvoie `True`, la tâche est *sautée* (delta-detection).
  C'est le cœur de l'idempotence : un run interrompu puis relancé ne refait que
  le travail restant.
- On marque `pending` avant le téléchargement, `done` (avec `hdfs_path`) après
  écriture réussie sur HDFS, `error` en cas d'échec (avec `$inc attempts` pour
  permettre un backoff / une reprise).

### Champs d'un document `file_state`

| Champ | Description |
|-------|-------------|
| `source`     | `nbb` \| `notaire` \| `ejustice` |
| `bce`        | numéro BCE 10 chiffres (zéro de tête) |
| `kind`       | `csv` \| `pdf` |
| `ref`        | identifiant unique du fichier dans `(source, bce, kind)` |
| `year`       | année de référence (dépôt / publication) |
| `reference`  | référence lisible du document (nom, référence de dépôt) |
| `status`     | `pending` \| `done` \| `error` |
| `hdfs_path`  | chemin absolu Bronze une fois écrit (rempli par `mark_done`) |
| `attempts`   | nombre de tentatives en échec (`$inc` par `mark_error`) |
| `error`      | dernier message d'erreur |
| `created_at` | horodatage de création (`$setOnInsert`, UTC *aware*) |
| `updated_at` | horodatage de dernière mise à jour (UTC *aware*) |

API du module `state_db.py` (toutes les fonctions reçoivent un `db` injecté —
un objet `Database` pymongo ou mongomock) :

```python
ensure_indexes(db)
is_done(db, source, bce, kind, ref) -> bool
mark_pending(db, source, bce, kind, ref, **meta)
mark_done(db, source, bce, kind, ref, hdfs_path, **meta)
mark_error(db, source, bce, kind, ref, error, **meta)
stats(db) -> dict            # compte groupé par (source, status)
```

> Les horodatages utilisent `datetime.now(timezone.utc)` (timezone-aware).

---

## 3. Disposition de la couche Bronze (HDFS)

Les destinations passées à `HdfsIO.put_bytes(data, rel)` sont des chemins
**relatifs** sous la racine Bronze (`HDFS_BRONZE`). `put_bytes` renvoie le chemin
absolu final (stocké dans `file_state.hdfs_path`).

```
{HDFS_BRONZE}/
├── nbb/
│   ├── pdf/{bce}/{year}_{reference}.pdf
│   └── csv/{bce}/{year}_{reference}.csv
├── notaire/
│   └── pdf/{bce}/{deedDate}_{documentId}.pdf
└── ejustice/
    └── pdf/{bce}/{numac}.pdf
```

`{bce}` = numéro BCE 10 chiffres **avec** le zéro de tête, ex. `0203430576` :

```
nbb/csv/0203430576/2024_REF.csv
notaire/pdf/0203430576/2023-05-12_ab12cd34.pdf
ejustice/pdf/0203430576/0123456789.pdf
```

Le backend HDFS est interchangeable via `HDFS_BACKEND` :

- `pyarrow` — `pyarrow.fs.HadoopFileSystem` (recommandé sur le cluster) ;
- `cli` — appelle `hdfs dfs -put/-test` ;
- `local` — écrit dans un dossier local (tests hors cluster).

---

## 4. Configuration (variables d'environnement)

Tout est surchargeable par l'environnement (mêmes fichiers en local et sur le
cluster). Définies dans `config.py` :

| Variable d'env | Constante `config.py` | Défaut | Rôle |
|----------------|-----------------------|--------|------|
| `BCE_MONGO_URI`             | `MONGO_URI`             | `mongodb://localhost:27017` | URI MongoDB |
| `BCE_MONGO_DB`              | `MONGO_DB`              | `bce`        | Base MongoDB |
| `BCE_COMPANIES_COLLECTION`  | `COMPANIES_COLLECTION`  | `companies`  | Collection référentiel entreprises |
| `BCE_STATE_COLLECTION`      | `STATE_COLLECTION`      | `file_state` | Collection State DB (idempotence) |
| `BCE_HDFS_BRONZE`           | `HDFS_BRONZE`           | `hdfs:///bronze` | Racine de la couche Bronze |
| `BCE_HDFS_BACKEND`          | `HDFS_BACKEND`          | `pyarrow`    | Backend d'écriture (`pyarrow`/`cli`/`local`) |
| `BCE_KBO_ENTERPRISE_CSV`    | `KBO_ENTERPRISE_CSV`    | `/data/kbo/enterprise.csv` | CSV KBO Open Data (seed) |
| `BCE_KBO_DENOMINATION_CSV`  | `KBO_DENOMINATION_CSV`  | `/data/kbo/denomination.csv` | CSV dénominations KBO (seed) |
| `BCE_NBB_DELAY`             | `NBB_DELAY`             | `0.6`  | Délai (s) entre requêtes NBB |
| `BCE_NOTAIRE_DELAY`         | `NOTAIRE_DELAY`         | `0.3`  | Délai (s) entre requêtes notaire |
| `BCE_EJUSTICE_DELAY`        | `EJUSTICE_DELAY`        | `0.4`  | Délai (s) entre requêtes eJustice |
| `BCE_HTTP_RETRIES`          | `HTTP_RETRIES`          | `5`    | Nombre de retentatives HTTP |
| `BCE_NOTAIRE_HEADLESS`      | `NOTAIRE_HEADLESS`      | `1`    | Playwright headless (cluster) |
| `BCE_BATCH_SIZE`            | `BATCH_SIZE`            | `200`  | Entreprises traitées par run de DAG |
| `BCE_YEAR_PDF_MIN`          | `YEAR_PDF_MIN`          | `2000` | Année minimale des PDF récupérés |
| `BCE_YEAR_CSV_MIN`          | `YEAR_CSV_MIN`          | `2021` | Année minimale des CSV récupérés |

---

## 5. Installation

```bash
pip install -r requirements.txt
# Obligatoire pour la source notaire.be (WAF F5) :
playwright install chromium
```

> **notaire.be est protégé par un WAF F5.** L'obtention d'un cookie valide passe
> par un navigateur *headless* (Playwright/Chromium). C'est pourquoi
> `playwright install chromium` est **indispensable**, et `BCE_NOTAIRE_HEADLESS=1`
> sur le cluster. Les cookies sont mis en cache (`notaire_cookies.json`) et
> renouvelés à l'expiration.

Le paquet doit être importable sous le nom `bce_ingestion` (sur le `PYTHONPATH`,
typiquement `pip install -e .` sur le cluster). Les imports internes sont
relatifs (`from . import config`, `from .hdfs_io import HdfsIO`, …).

---

## 6. Exécution

### (1) Peupler MongoDB (seed) depuis le KBO Open Data

```bash
export BCE_KBO_ENTERPRISE_CSV=/data/kbo/enterprise.csv
python -m bce_ingestion.seed_companies --limit 10000
```

Charge la collection `companies` (numéro BCE + métadonnées) qui pilote ensuite
l'ingestion.

### (2) Déployer les DAGs sur Airflow

Placer le paquet sur le `PYTHONPATH` d'Airflow et exposer les DAGs dans le
`dags_folder` (par ex. via un lien symbolique vers `bce_ingestion/dags/`), puis
installer les dépendances dans l'environnement des workers :

```bash
pip install -e /chemin/vers/bce_ingestion
playwright install chromium          # pour le worker qui traite notaire
```

### (3) Déclencher

Activer / déclencher les DAGs depuis l'UI Airflow (ou `airflow dags trigger`).
Chaque run traite un lot de `BATCH_SIZE` entreprises ; grâce à la State DB, les
runs successifs ne récupèrent que le *delta* (documents pas encore en `done`).

---

## 7. Structure du paquet

```
bce_ingestion/
├── config.py          # configuration surchargeable par env  (fourni)
├── hdfs_io.py         # écriture Bronze HDFS (pyarrow/cli/local)  (fourni)
├── state_db.py        # State DB d'idempotence (injection de `db`)
├── seed_companies.py  # seed MongoDB depuis KBO Open Data
├── sources/           # scrapers par source (nbb, notaire, ejustice)
├── dags/              # DAGs Airflow (un par source)
├── requirements.txt
└── README.md
```

---

## 8. Tests hors cluster

- MongoDB : `mongomock.MongoClient()['bce']` (pymongo n'est pas importé au niveau
  module — imports paresseux à l'intérieur des fonctions).
- HDFS : `HDFS_BACKEND=local` écrit dans un dossier local.
- Toute la logique DB accepte un `db` **injecté**, testable avec `mongomock`.

---

## 9. Jour 2 — couche Silver + ciblage Hôtellerie & scraping NBB

Le présent README couvre la **couche Bronze** (Jour 1). Le **Jour 2** ajoute,
**sans modifier le Bronze**, une couche **Silver** (`enterprise_silver` : dates
ISO, dédup activités, adresse `REGO`, dénomination officielle, libellés FR
décodés), un **ciblage sectoriel hôtellerie** (9 codes NACE de la division 55) et
un **scraping NBB idempotent au niveau entreprise** (`scrape_state` +
`file_state`) déposant les CSV comptables `>= 2021` dans le Bronze HDFS
(`{bce}/nbb/{year}/{ref}.csv`).

Nouveaux modules : `build_bronze.py`, `codes.py`, `silver.py`, `hotel.py`,
`scrape_nbb.py`, `dags/bce_silver_dag.py`, `dags/bce_hotel_nbb_dag.py`.

Détails complets : voir [`README_JOUR2.md`](README_JOUR2.md).
