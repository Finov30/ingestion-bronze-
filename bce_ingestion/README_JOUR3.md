# JOUR 3 — Couche GOLD + API FastAPI + Frontend React + DAG de recalcul annuel

Ce document décrit la **troisième journée** du pipeline BCE / hôtellerie belge. Les
jours précédents ont construit le paquet Python `bce_ingestion` :

- **Jour 1** — couche **Bronze** (ingestion KBO Open Data → MongoDB `enterprise_finale`,
  écriture HDFS des CSV/PDF, State DB par fichier).
- **Jour 2** — couche **Silver** (`enterprise_silver` : documents nettoyés/enrichis),
  ciblage sectoriel **hôtellerie** (division NACE 55), et scraping des dépôts NBB
  (`consult.cbso.nbb.be`) vers HDFS `{bronze}/nbb/{year}/{ref}.csv`, piloté par la
  State DB au niveau entreprise (`scrape_state` : `pending`/`in_progress`/`done`).

Le **Jour 3** ajoute :

1. La couche **GOLD** — parsing PCMN des CSV NBB → indicateurs financiers + ratios,
   matérialisés dans la collection MongoDB `hotel_gold` (un document par entreprise,
   un tableau `years[]`).
2. Un **backend FastAPI** exposant Silver + Gold + dirigeants (kbopub) + statuts
   (notaire) à travers une API JSON + SSE.
3. Un **frontend React** (Vite + Redux Toolkit) : recherche, fiche entreprise,
   graphe Sankey financier, streaming progressif des statuts.
4. Un **DAG Airflow** `bce_gold_recalc` — recalcul incrémental annuel de la Gold,
   idempotent via la State DB.

---

## 0. Architecture générale

```
                       KBO Open Data (CSV)          consult.cbso.nbb.be
                              │                       (dépôts NBB, CSV)
                              ▼                              │
   ┌──────────────────────────────────────────┐            │  scrape_nbb (Jour 2)
   │  BRONZE   MongoDB enterprise_finale        │            ▼
   │           HDFS  {bronze}/nbb/{year}/{ref}.csv  ◀────────┘
   └───────────────┬────────────────────────────┘
                   │ silver.py (Jour 2)
                   ▼
   ┌──────────────────────────────────────────┐
   │  SILVER   MongoDB enterprise_silver        │
   │           (denomination_principale, address, activities…)
   └───────────────┬────────────────────────────┘
                   │  hotel.py → scrape_state (pending/done)
                   │
   ┌═══════════════▼══════════════════════════════════════════════════════┐
   ║                          JOUR 3 — GOLD                                 ║
   ║                                                                        ║
   ║   HDFS {bronze}/nbb/{year}/{ref}.csv                                   ║
   ║        │  gold_spark.py  (lecture Spark, 1 partition = 1 CSV)          ║
   ║        │      └─ appelle gold.py (PCMN pur, testable sans Spark)       ║
   ║        ▼                                                               ║
   ║   MongoDB hotel_gold  { enterprise_number(_id), years:[…], ratios }    ║
   ║        │  upsert on enterprise_number                                  ║
   ╚════════▼═══════════════════════════════════════════════════════════════╝
            │
            ▼
   ┌──────────────────────────────────────────┐        ┌───────────────────┐
   │  FastAPI backend (uvicorn :8000)          │◀──────▶│  kbopub (dirigeants)│
   │  /api/health /search /enterprise/{bce}    │        │  notaire (statuts)  │
   │  /…/dirigeants  /…/statutes/stream (SSE)  │        └───────────────────┘
   └───────────────┬────────────────────────────┘
                   │  HTTP JSON + text/event-stream
                   ▼
   ┌──────────────────────────────────────────┐
   │  React frontend (Vite dev :5173)          │
   │  Redux Toolkit — Search / Fiche / Statuts │
   │  Sankey CA→Marge brute→Résultat net       │
   └──────────────────────────────────────────┘
```

Le flux nominal Gold est donc : **HDFS CSV → Spark Gold → `hotel_gold` → FastAPI → React**.

---

## 1. Couche GOLD

### 1.1 Objectif

Transformer les comptes annuels bruts déposés à la **BNB / Centrale des bilans**
(CSV PCMN — Plan Comptable Minimum Normalisé) en un **document analytique par
entreprise**, contenant l'historique pluriannuel des grands indicateurs financiers
et cinq ratios calculés, prêt à être servi tel quel par l'API et affiché par le
frontend (aucun calcul lourd côté client).

Deux fichiers implémentent la logique, avec une séparation stricte
**calcul pur / orchestration distribuée** (même discipline qu'au Jour 2) :

| Fichier | Rôle | Dépendances |
|---|---|---|
| `bce_ingestion/gold.py` | **Logique métier PURE** : parsing d'un CSV NBB, mapping PCMN → champs, calcul des ratios, construction du document `hotel_gold`. Testable sans Spark ni Mongo. | stdlib uniquement (+ import paresseux éventuel) |
| `bce_ingestion/gold_spark.py` | **Job Spark** : lit les CSV depuis HDFS, distribue le parsing (`gold.py`), agrège par entreprise, **upsert** dans `hotel_gold`. | pyspark, pymongo (injecté) |

### 1.2 Configuration ajoutée (`config.py`)

```python
# --- Jour 3 : couche Gold ---
GOLD_COLLECTION = os.environ.get("BCE_GOLD_COLLECTION", "hotel_gold")
```

### 1.3 Format réel des CSV NBB (à parser)

Les CSV téléchargés sur `consult.cbso.nbb.be` sont **séparés par des virgules,
entièrement guillemetés et SANS ligne d'en-tête**. Structure :

- **~18 premières lignes = métadonnées** sous forme de paires `("clé","valeur")` :
  - `("Reference number", "...")`
  - `("Entity number", "0878065378")`
  - `("Accounting period end date", "2024-12-31")` → **l'année fiscale** = année de
    cette date (ici `2024`).
  - `("Model code", "m02-f")` → détermine le `schema_type`.
  - `("Legal form", "...")`, etc.
- **Lignes de données = paires `("CODE_PCMN","valeur")`**, ex. `("70","123.4")`.

Règles de nettoyage :

- **Codes suffixés `P`** = colonne comparative **N-1** → **on les IGNORE** (on ne
  garde que l'exercice courant N).
- Les valeurs sont des **flottants** : gestion des décimales à la **virgule** comme
  au **point** (`"1.234,56"` / `"1234.56"` → `1234.56`) ; valeur absente ou non
  numérique → `0.0`.

Mapping du **`Model code`** vers `schema_type` :

| Model code (préfixe) | `schema_type` |
|---|---|
| `m01`, `m81` | `full` (comptes complets) |
| `m02`, `m82` | `abrege` (schéma abrégé) |
| `m11`, `m12`, `micro` | `micro` (micro-entreprise) |
| tout autre | `abrege` (défaut) |

### 1.4 Mapping PCMN → champs Gold

Chaque exercice (année) produit les indicateurs suivants. Les codes manquants
valent **`0.0`** par défaut.

| Champ Gold | Code(s) PCMN | Règle |
|---|---|---|
| `ca` (chiffre d'affaires) | `70` | valeur directe |
| `achats` (usage interne ratios) | `60` | valeur directe |
| `variation_stocks` (usage interne ratios) | `71` | valeur directe |
| `ebit` | `9901` | valeur directe |
| `resultat_net` | `9904` | valeur directe |
| `tresorerie` | `54` + `55` | somme |
| `dettes_financieres` | `17` + `43` | somme |
| `fonds_propres` | `10/15` | code agrégé ; **repli** = somme `10`…`15` |
| `capital_souscrit` | `100` | valeur directe |
| `marge_brute` | `70` − `60` + `71` | = `ca − achats + variation_stocks` |

### 1.5 Les 5 ratios (par année)

Toutes les divisions sont **protégées** : si le dénominateur vaut `0`, le ratio
vaut **`None`** (`null` en JSON).

| Ratio | Formule | Unité |
|---|---|---|
| `marge_nette` | `resultat_net / ca * 100` | % |
| `roe` | `resultat_net / fonds_propres * 100` | % |
| `ratio_liquidite` | `tresorerie / dettes_financieres` | ratio |
| `taux_endettement` | `dettes_financieres / fonds_propres * 100` | % |
| `marge_brute` (indicateur, cf. 1.4) | `ca − achats + variation_stocks` | € |

> Note : `marge_brute` est stockée comme **montant** au niveau de l'année (pas dans
> `ratios`), tandis que `marge_nette`, `roe`, `ratio_liquidite`, `taux_endettement`
> vivent dans le sous-objet `ratios`.

### 1.6 Schéma du document `hotel_gold` (un doc par entreprise)

`UPSERT` sur `enterprise_number` (qui **EST** le `_id`, le bce10 avec zéro de tête).

```jsonc
{
  "enterprise_number": "0878065378",   // == _id
  "schema_type": "abrege",             // full | abrege | micro (dernier exercice)
  "last_updated": "2026-07-02T…Z",     // datetime UTC de la dernière écriture
  "years": [                           // TRIÉ par year croissant
    {
      "year": 2023,
      "ca": 1234567.0,
      "marge_brute": 456789.0,
      "ebit": 120000.0,
      "resultat_net": 89000.0,
      "tresorerie": 45000.0,
      "dettes_financieres": 300000.0,
      "fonds_propres": 500000.0,
      "capital_souscrit": 100000.0,
      "ratios": {
        "marge_nette": 7.21,           // %  (null si ca == 0)
        "roe": 17.8,                   // %  (null si fonds_propres == 0)
        "ratio_liquidite": 0.15,       //    (null si dettes_financieres == 0)
        "taux_endettement": 60.0       // %  (null si fonds_propres == 0)
      }
    },
    { "year": 2024, "…": "…" }
  ]
}
```

Un document = **toute l'histoire financière** d'une entreprise. À chaque nouveau
dépôt (nouvelle année), on **fusionne** l'année dans `years[]` puis on ré-`upsert`.

### 1.7 `gold.py` — API pure (extrait fonctionnel attendu)

```python
parse_nbb_csv(csv_text) -> {
    "entity_number": str, "year": int, "schema_type": str, "codes": {code: float}
}
build_year(codes) -> {year, ca, marge_brute, ebit, resultat_net, tresorerie,
                      dettes_financieres, fonds_propres, capital_souscrit, ratios{…}}
compute_ratios(ca, achats, var_stocks, resultat_net, tresorerie,
               dettes_financieres, fonds_propres) -> {marge_nette, roe,
                      ratio_liquidite, taux_endettement}   # divisions gardées
build_gold_doc(enterprise_number, years, schema_type) -> {doc hotel_gold}
```

Aucune de ces fonctions n'importe `pyspark` ni `pymongo` → tests unitaires directs
(mêmes fixtures que les CSV réels : `/mnt/c/.../bce_nbb_data/nbb/csvs/{bce}/{year}.csv`).

### 1.8 `gold_spark.py` — job Spark

Principe (aligné sur les jobs Bronze/Silver du projet) :

1. Lister les CSV Gold à traiter depuis HDFS (`{bronze}/nbb/{year}/{ref}.csv`) ou,
   en incrémental, uniquement ceux d'un batch/année (cf. DAG §1.9).
2. `spark.sparkContext.wholeTextFiles(...)` → un enregistrement `(chemin, contenu)`
   par CSV (une partition ≈ un fichier), puis `map(gold.parse_nbb_csv)`.
3. `groupBy(entity_number)` → agrège les années d'une même entreprise, tri par
   `year`, `gold.build_gold_doc(...)`.
4. Écriture : pour chaque doc, `hotel_gold.update_one({"_id": bce}, {"$set": doc},
   upsert=True)` (le `db` pymongo est **injecté**, jamais importé au niveau module,
   pour rester testable en mongomock).

Fusion incrémentale : une nouvelle année ne remplace pas le document ; elle est
insérée/mise à jour dans `years[]` (merge par `year`), le tableau est re-trié, puis
`schema_type` et `last_updated` sont rafraîchis.

### 1.9 DAG `bce_gold_recalc` — recalcul incrémental annuel

`dags/bce_gold_recalc.py`. Objectif : chaque année (après la campagne de dépôts NBB),
recalculer la Gold **uniquement pour les entreprises ayant un nouveau CSV**, de façon
idempotente.

- **Détection de delta** via la **State DB par fichier** (`state_db`, collection
  `file_state`) : on ne (re)traite en Gold que les CSV `source='nbb'`, `kind='csv'`,
  `status='done'` **non encore marqués Gold** — matérialisé par un champ/statut
  dédié (ex. `gold_done: True` posé après upsert réussi) ou par comparaison de
  `updated_at`. Relancer le DAG ne recrée jamais de doublon (tout passe par
  `update_one(..., upsert=True)`).
- **Batching** : `BATCH_SIZE` entreprises par run (comme les DAG Jour 2), balayage
  au fil des exécutions.
- **Schedule** : `@yearly` (ou `cron` début d'année civile), après la fenêtre de
  dépôt légal des comptes annuels.
- **Tâches** : `list_pending_gold` → `run_gold_spark` (soumet `gold_spark.py`) →
  `mark_gold_done` (met à jour la State DB).

---

## 2. Backend FastAPI

Application ASGI servie par **uvicorn** (port 8000 par défaut). Toutes les fonctions
d'accès Mongo **injectent** la base (testable en mongomock/httpx), imports `pymongo`
paresseux. **CORS** : autorise l'origine du dev Vite `http://localhost:5173` et `*`
pour simplifier.

### 2.1 Table des endpoints (contrat API)

| Méthode / Route | Réponse | Source de données |
|---|---|---|
| `GET /api/health` | `{"status":"ok"}` | — |
| `GET /api/search?q=<name\|bce>&limit=20` | `{"results":[{enterprise_number, denomination, status_label, juridical_form_label}]}` | `enterprise_silver` |
| `GET /api/enterprise/{bce}` | `{enterprise_number, silver{…}\|null, gold{…}\|null}` | `enterprise_silver` + `hotel_gold` |
| `GET /api/enterprise/{bce}/dirigeants` | `{"dirigeants":[{nom, qualites:[str]}], "cached":bool}` | cache Mongo `dirigeants` sinon **kbopub** |
| `GET /api/enterprise/{bce}/statutes/stream` | **SSE** `text/event-stream` | cache Mongo `statutes` sinon **notaire** |

**`/api/search`** — logique : si `q` est **numérique** → match sur **préfixe de
`_id`** (bce10) ; sinon **regex insensible à la casse** sur `denomination_principale`
(et le tableau `denominations`). Limité par `limit` (défaut 20).

**`/api/enterprise/{bce}`** — enveloppe :

```jsonc
{
  "enterprise_number": "0878065378",
  "silver": {                          // depuis enterprise_silver, ou null
    "denomination_principale": "…",
    "StatusLabel": "…", "JuridicalFormLabel": "…", "StartDate": "YYYY-MM-DD",
    "address": {"StreetFR","HouseNumber","Zipcode","MunicipalityFR"} | null,
    "activities": [{"NaceCode","Classification","NaceLabel"}]
  },
  "gold": {                            // depuis hotel_gold, ou null
    "schema_type": "abrege", "last_updated": "…",
    "years": [ { "year", "ca", "marge_brute", "ebit", "resultat_net",
                 "tresorerie", "dettes_financieres", "fonds_propres",
                 "capital_souscrit",
                 "ratios": {"marge_nette","roe","ratio_liquidite","taux_endettement"} } ]
  }
}
```

Une entreprise sans comptes NBB renvoie `"gold": null` (fiche Silver seule).

### 2.2 Dirigeants — cache kbopub

`GET /api/enterprise/{bce}/dirigeants`.

1. Chercher dans la collection Mongo **`dirigeants`** (clé `_id = bce`). Si présent →
   renvoyer `{"dirigeants":[…], "cached": true}` **instantanément**.
2. Sinon : **scraper kbopub** (page publique `kbopub.economie.fgov.be` de la BCE →
   liste « Fonctions » : nom + qualités/mandats), **persister** dans `dirigeants`, et
   renvoyer avec `"cached": false`.

Le scraping est encapsulé (parsing HTML) ; en cas d'échec réseau on renvoie une liste
vide plutôt que de faire planter la requête.

### 2.3 Statuts — streaming SSE (`/statutes/stream`)

Renvoie un flux **Server-Sent Events** (`media_type="text/event-stream"`). Design :

- **Cache** : si la collection Mongo **`statutes`** contient déjà les statuts du bce,
  on les **stream instantanément** (un event par statut) sans rien scraper.
- **Sinon** : on lance le scraping via `bce_ingestion.sources.strapor_notaire`
  (`get_statutes(bce10)`) **dans un thread** (le navigateur headless Playwright /
  polling F5 est bloquant), et on **émet chaque statut au fil de l'eau**, puis on
  **persiste** le résultat dans `statutes`.
- **Format des events** — une ligne par statut :

  ```
  data: {"document": "...", "date": "...", "notaire": "...", "statut": "...", "documentId": "..."}\n\n
  ```

  (Chaque objet est projeté depuis le retour de `get_statutes`, dont les champs
  bruts sont `documentTitle`, `deedDate`, `organizationName`, `documentStatus`,
  `documentId`.)

- **Fin de flux** :

  ```
  event: done
  data: {"count": N}
  ```

- **Erreur scraper** (pas de crash serveur) :

  ```
  event: error
  data: {"message": "..."}
  ```

  puis fermeture propre du flux.

### 2.4 Proxies Tor (optionnel, OFF par défaut)

Les portails publics (NBB, notaire, kbopub) limitent le débit / peuvent bloquer une
IP. Le backend prévoit un **support optionnel de proxies (Tor / pool)** pour les
appels de scraping, **désactivé par défaut** (`TOR_PROXIES` vide / flag OFF). En prod
on peut router les requêtes de scraping via un `socks5h://127.0.0.1:9050` sans toucher
au reste du code.

---

## 3. Frontend React

**Vite + React + Redux Toolkit**. Trois vues, un store centralisé, appels vers le
backend FastAPI (proxy `/api` → `:8000` en dev).

### 3.1 Stack & organisation

- **Vite** (dev server `http://localhost:5173`, `npm run dev`).
- **Redux Toolkit** : slices `search`, `enterprise` (silver+gold), `statutes`
  (progressif SSE), `dirigeants` ; thunks async pour les appels API.
- Un client API léger (`fetch`) + un client SSE (`EventSource`) pour le flux statuts.

### 3.2 Vues

| Vue | Contenu | Source(s) affichée(s) |
|---|---|---|
| **Search** | Barre de recherche (nom **ou** bce) → liste de résultats cliquables. | `/api/search` → `enterprise_silver` |
| **Fiche** | Identité (dénomination, statut, forme juridique, date de début, adresse, activités NACE), bloc financier + **Sankey**, encart **dirigeants**. | Silver + **Gold** + kbopub |
| **Statutes** | Liste des statuts notariés qui se **remplit progressivement** (spinner tant que le flux n'est pas terminé). | SSE → cache/notaire |

Chaque section indique sa **provenance de données** : `silver` (identité),
`gold` (finances/ratios/Sankey), `kbopub` (dirigeants), `notaire` (statuts).

### 3.3 Graphe Sankey financier (3 nœuds)

Diagramme de Sankey à **3 nœuds** matérialisant la cascade de valeur de l'exercice
sélectionné :

```
   Chiffre d'affaires ──▶ Marge brute ──▶ Résultat net
        (ca)                (marge_brute)     (resultat_net)
```

- **Sélecteur d'année** : dropdown alimenté par `gold.years[].year` ; changer l'année
  recompose le Sankey à partir des montants déjà présents dans le document Gold
  (aucun recalcul serveur).
- Les valeurs proviennent directement de l'année Gold choisie (`ca`, `marge_brute`,
  `resultat_net`) ; les ratios (`marge_nette`, `roe`, `ratio_liquidite`,
  `taux_endettement`) sont affichés en cartouches à côté du graphe.

### 3.4 Chargement progressif SSE (statuts)

La vue Statutes ouvre un `EventSource` sur `/api/enterprise/{bce}/statutes/stream` :

- chaque `data:` reçu ➜ `dispatch(addStatute(…))` (la liste grossit en direct) ;
- **spinner** affiché tant que l'event `done` n'est pas reçu ;
- event `error` ➜ message d'erreur non bloquant, fermeture du flux.

Le cache backend fait qu'un second affichage est **instantané**.

---

## 4. Instructions d'exécution

> Prérequis : MongoDB (`bce`) peuplée jusqu'au Silver + scraping NBB (Jours 1–2),
> HDFS avec les CSV `{bronze}/nbb/{year}/{ref}.csv`. `pyspark 4.1.2`, `fastapi`,
> `uvicorn`, `httpx`, Node v24 + npm 11.

### 4.1 Construire / recalculer la couche Gold

```bash
# Job Spark local (ou spark-submit sur cluster)
export BCE_MONGO_URI="mongodb://localhost:27017"
export BCE_MONGO_DB="bce"
export BCE_HDFS_BRONZE="hdfs:///bronze"
python -m bce_ingestion.gold_spark            # lit HDFS → upsert hotel_gold

# ou via le DAG incrémental annuel (Airflow)
#   dags/bce_gold_recalc.py   (schedule @yearly, batch BCE_BATCH_SIZE)
```

### 4.2 Démarrer l'API (uvicorn)

```bash
pip install fastapi uvicorn httpx          # fastapi absent par défaut
uvicorn bce_api.main:app --reload --port 8000
# vérif :  curl http://localhost:8000/api/health   ->  {"status":"ok"}
```

### 4.3 Lancer le frontend (Vite)

```bash
cd frontend
npm install
npm run dev                                # http://localhost:5173
```

Le frontend consomme `http://localhost:8000/api/*` (CORS autorisé côté backend).

---

## 5. Résumé des ajouts Jour 3

| Élément | Emplacement | Rôle |
|---|---|---|
| `gold.py` | `bce_ingestion/gold.py` | Parsing PCMN pur + ratios + doc `hotel_gold` |
| `gold_spark.py` | `bce_ingestion/gold_spark.py` | Job Spark HDFS → `hotel_gold` (upsert) |
| `GOLD_COLLECTION` | `config.py` | `hotel_gold` |
| `bce_gold_recalc` | `dags/bce_gold_recalc.py` | Recalcul annuel incrémental (State DB) |
| Backend FastAPI | `bce_api/` | health/search/enterprise/dirigeants/statutes(SSE) |
| Frontend React | `frontend/` | Vite + Redux : Search / Fiche / Statutes + Sankey |

Le tout reste fidèle aux conventions du projet : **`db` injecté**, imports lourds
(`pymongo`, `pyspark`, `pandas`) **paresseux**, **idempotence** via `state_db`
(`update_one(..., upsert=True)`), configuration **surchargeable par variables
d'environnement** (`config.py`).
