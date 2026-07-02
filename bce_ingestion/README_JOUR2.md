# bce_ingestion — Jour 2 : couche Silver + ciblage Hôtellerie & scraping NBB

> Ce document décrit **Jour 2** du pipeline `bce_ingestion` (medallion). Le
> Jour 1 (couche **Bronze** brute : `config.py`, `hdfs_io.py`, `mongo.py`,
> `state_db.py`, `ingest.py`, `sources/*`, DAGs) est documenté dans
> [`README.md`](README.md) et reste **inchangé**. Jour 2 **ajoute** une couche
> **Silver** dérivée du Bronze riche, un **ciblage sectoriel** (hôtellerie) et
> un **scraping NBB idempotent** au niveau entreprise.

```
KBO Open Data (7 CSV + code)                  consult.cbso.nbb.be (API)
        │                                              ▲
        │ build_bronze                                 │ scrape_nbb (>= 2021)
        ▼                                              │
 enterprise_finale  ──build_silver──►  enterprise_silver ──hotel──► cibles Horeca
   (Bronze riche)      (5 transforms)     (nettoyé/enrichi)    (9 codes NACE 55)
                                                                    │
                                                                    ▼
                                     Bronze HDFS  {bce}/nbb/{year}/{ref}.csv
                                                  + StateDB entreprise (scrape_state)
                                                  + StateDB fichier   (file_state)
```

---

## 1. Rationale de la couche Silver

La medallion sépare strictement les responsabilités :

| Couche | Collection Mongo | Contenu | Mutabilité |
|--------|------------------|---------|------------|
| **Bronze (riche)** | `enterprise_finale` (`config.FINALE_COLLECTION`) | Document par entreprise, **fidèle au KBO Open Data** : entreprise + dénominations / adresses / activités / contacts / établissements / succursales **imbriqués bruts** (toutes langues, toutes versions NACE, tous types d'adresse). On archive la donnée telle que servie, sans jugement. | **Intangible.** Jamais réécrit par Silver. Sert de source rejouable. |
| **Silver** | `enterprise_silver` (`config.SILVER_COLLECTION`) | Copie **nettoyée, dédupliquée et enrichie** (labels FR décodés) d'`enterprise_finale`. C'est la couche « prête à requêter » sur laquelle s'appuie le ciblage sectoriel. | **Reconstructible** à tout moment depuis `enterprise_finale` (aucune information n'est perdue si on la régénère). |

Principe clé : **Bronze reste intact.** `build_silver` **lit** `enterprise_finale`
et **écrit** `enterprise_silver` (upsert par `_id` = BCE 10 chiffres). On peut
re-générer Silver autant de fois qu'on veut sans jamais re-télécharger ni altérer
le Bronze — exactement comme la State DB garantit l'idempotence du Bronze.

---

## 2. Les 5 transformations Silver

`silver.py` applique cinq transformations déterministes à chaque document
`enterprise_finale`. Le décodage des libellés (transform 5) s'appuie sur
`codes.py` (index de `code.csv`, langue **FR**).

### Transform 1 — Normalisation des dates `DD-MM-YYYY → YYYY-MM-DD`

Le KBO écrit les dates au format belge `DD-MM-YYYY`. Silver les convertit en
ISO 8601 (triables, comparables) : `StartDate`, `DateStrikingOff`, etc.

```
avant :  start_date = "09-08-1960"
après :  start_date = "1960-08-09"
```

### Transform 2 — Déduplication des activités (conserver 70220 & 70200 et MAIN+SECO)

`enterprise_finale` empile les activités **toutes versions NACE confondues**
(`Nace2003` / `Nace2008` / `Nace2025`) et **tous groupes d'activité** (`001`
Activités TVA, `006` …). Le même code réapparaît donc plusieurs fois. La règle
de dédup collapse uniquement les **doublons exacts** sur la clé
`(NaceCode, Classification)` : on **conserve les codes distincts** et on
**conserve les deux classifications** `MAIN` et `SECO` d'un même code.

```
avant (imbriqué, brut) :
  [ {NaceVersion:2008, NaceCode:70220, Classification:MAIN},
    {NaceVersion:2025, NaceCode:70220, Classification:MAIN},   ← doublon de version
    {NaceVersion:2025, NaceCode:70200, Classification:MAIN},
    {NaceVersion:2025, NaceCode:70220, Classification:SECO} ]

après (dédupliqué) :
  [ {NaceCode:70220, Classification:MAIN},   ← 2008+2025 fusionnés
    {NaceCode:70200, Classification:MAIN},   ← code distinct  → conservé
    {NaceCode:70220, Classification:SECO} ]  ← même code, autre rôle → conservé
```

On garde donc **70220 ET 70200** (codes différents) **et MAIN+SECO** (rôles
différents du même code) ; seule la répétition inter-versions est éliminée.

### Transform 3 — Adresse : siège social uniquement (`REGO`)

`address.csv` peut contenir plusieurs `TypeOfAddress`. Silver ne conserve que
l'adresse **`REGO`** (Registered Office / siège social) et l'aplatit en un
sous-document propre.

```
avant :  addresses = [ {TypeOfAddress:"REGO", Zipcode:"9070", MunicipalityNL:"Destelbergen", …},
                       {TypeOfAddress:"OTHER", …} ]
après :  address    =   {type:"REGO", zipcode:"9070", municipality:"Destelbergen",
                         street:"Panhuisstraat", house_number:"1", box:null, country:"BE"}
```

### Transform 4 — Dénomination officielle (`TypeOfDenomination = 001`)

`denomination.csv` distingue `001` Dénomination (officielle), `002` Abréviation,
`003` Dénomination commerciale, `004` Nom de succursale. Silver ne retient que la
**dénomination officielle `001`**, en préférant le **français (`Language=1`)** et
en se rabattant sur le **néerlandais (`Language=2`)** à défaut.

```
avant :  denominations = [ {Type:"001", Lang:"2", "Intergemeentelijke Vereniging Veneco"},
                           {Type:"002", Lang:"2", "Veneco"} ]
après :  denomination   = "Intergemeentelijke Vereniging Veneco"   (001, FR>NL)
```

### Transform 5 — Décodage des libellés via `code.csv` (FR)

Les codes techniques sont **enrichis** de leur libellé français (sans supprimer
le code brut) grâce à `codes.py` :

| Champ codé | Catégorie `code.csv` | Exemple → libellé FR |
|------------|----------------------|----------------------|
| `status` | `Status` | `AC` → **Actif** |
| `type` | `TypeOfEnterprise` | `2` → **Personne morale** |
| `juridical_form` | `JuridicalForm` | `014` → **Société anonyme** |
| `activities[].nace_code` | `Nace2025` / `Nace2008` / `Nace2003` | `55100` → **Hôtels et hébergement similaire** |

```
avant :  status = "AC"          juridical_form = "014"     nace_code = "55100"
après :  status = "AC", status_label = "Actif"
         juridical_form = "014", juridical_form_label = "Société anonyme"
         activities[i].nace_label = "Hôtels et hébergement similaire"
```

> Langues de `code.csv` : **`1`/`FR` = français, `2`/`NL` = néerlandais** (vérifié
> sur `Category='Language'`). Silver décode systématiquement en **FR**.

### Schéma résultant d'un document `enterprise_silver`

```jsonc
{
  "_id": "0203430576",                 // BCE 10 chiffres (clé)
  "bce_formatted": "0203.430.576",
  "status": "AC", "status_label": "Actif",
  "type": "2",  "type_label": "Personne morale",
  "juridical_form": "014", "juridical_form_label": "Société anonyme",
  "start_date": "1960-08-09",          // transform 1
  "denomination": "SNCB",              // transform 4 (001, FR>NL)
  "address": {                          // transform 3 (REGO uniquement)
    "type": "REGO", "zipcode": "1060", "municipality": "Saint-Gilles",
    "street": "Rue de France", "house_number": "56", "box": null, "country": "BE"
  },
  "activities": [                       // transform 2 + transform 5
    {"nace_version": "2025", "nace_code": "55100",
     "classification": "MAIN", "nace_label": "Hôtels et hébergement similaire"}
  ],
  "silver_built_at": "2026-07-01T00:00:00Z"
}
```

---

## 3. Ciblage sectoriel — Hôtellerie (`hotel.py`)

Objectif : isoler les entreprises du **secteur de l'hébergement (Horeca /
hôtellerie)** qui valent la peine d'être enrichies financièrement.

### Les 9 codes NACE ciblés (division 55 — Hébergement, NACE-BEL 2025)

| Code NACE | Libellé (FR) |
|-----------|--------------|
| `55100` | Hôtels et hébergement similaire |
| `55201` | Auberges pour jeunes |
| `55202` | Centres et villages de vacances |
| `55203` | Gîtes de vacances, appartements et meublés de vacances |
| `55204` | Chambres d'hôtes |
| `55209` | Hébergement touristique et autre hébergement de courte durée n.c.a. |
| `55300` | Terrains de camping et parcs pour caravanes ou véhicules de loisirs |
| `55400` | Activités de service d'intermédiation pour l'hébergement |
| `55900` | Autres hébergements |

Ces 9 codes sont figés dans `hotel.HOTEL_NACE_CODES`.

### Filtres appliqués (sur `enterprise_silver`)

Une entreprise est **ciblée** si **toutes** les conditions sont réunies :

1. **`status == "AC"`** — uniquement les entreprises **actives** ;
2. **`type == "2"`** — uniquement les **personnes morales** (les personnes
   physiques ne déposent pas de comptes à la NBB) ;
3. **au moins une activité `MAIN`** dont le `nace_code` figure dans les 9 codes
   ci-dessus → **`Classification = MAIN` uniquement** (on écarte les activités
   `SECO` : on veut des acteurs dont l'hébergement est le métier **principal**) ;
4. **`juridical_form` hors liste d'exclusion** (voir ci-dessous).

### Formes juridiques exclues (regroupées)

On écarte les formes qui, par nature, ne sont **pas des exploitants hôteliers
commerciaux** ou ne déposent pas de comptes annuels exploitables :

| Groupe | Codes `JuridicalForm` (exemples) | Raison de l'exclusion |
|--------|----------------------------------|-----------------------|
| Sans but lucratif / fondations | `017` ASBL, `018` Établissement d'utilité publique, `026` Fondation privée, `028` Institution sans but lucratif, `029` Fondation d'utilité publique | Non commercial, régime comptable distinct |
| Structures publiques / para-régionales | `416` Association prestataire de services (Rég. flamande), `417` Association chargée de mission | Secteur public, pas un hôtelier privé |
| Entités sans personnalité juridique | `701`–`704`, `706`, `721`–`724` (sociétés/associations sans personnalité juridique, momentanées, de frais, syndicats) | Ne déposent pas de comptes propres |
| Structures TVA / copropriété / agricole | `003` Unité TVA, `070` Association des copropriétaires, `025` Société agricole | Hors périmètre hôtelier |

La liste exhaustive vit dans `hotel.EXCLUDED_JURIDICAL_FORMS`. `hotel.select_hotels(db)`
renvoie les BCE (10 chiffres) retenus, prêts à alimenter le scraping NBB.

---

## 4. Scraping NBB (comptes annuels ≥ 2021) — `scrape_nbb.py`

Pour chaque entreprise ciblée, on télécharge les **CSV comptables** publiés à la
NBB/CBSO et on les archive dans le **Bronze HDFS**.

### Flux par entreprise

```
1. scrape_state : marquer l'entreprise 'in_progress'
2. consult_nbb.make_session(bce)  +  get_all_deposits(session, bce)   (paginé, poli)
3. filtrer : accountingYearEndDate >= 2021          (== periodEndDateYear >= YEAR_CSV_MIN)
             et écarter les dépôts consolidés / migrés (pas de CSV exploitable)
4. pour chaque dépôt retenu :
      dest = "{bce}/nbb/{year}/{ref}.csv"
      si file_state.is_done(nbb,bce,csv,ref)  ou  hdfs.exists(dest)  → SKIP
      sinon : fetch_csv_text(session, deposit_id) → hdfs.put_bytes → file_state.mark_done
5. scrape_state : marquer 'done', filings_count = nb de CSV en 'done'
```

> Le champ NBB **`accountingYearEndDate`** de l'énoncé correspond à la date de
> clôture de l'exercice du dépôt, exposée par l'API sous
> **`periodEndDate`** (`2025-12-31T…`) / **`periodEndDateYear`** (`2025`). Le seuil
> `>= 2021` est piloté par **`config.YEAR_CSV_MIN`** (Jour 1).

### Disposition Bronze (Jour 2, orientée entreprise)

Alors que le Jour 1 range les CSV en `nbb/csv/{bce}/…`, le scraping hôtellerie
utilise une arborescence **`{bce}` en tête** (regroupement par entreprise ciblée) :

```
{HDFS_BRONZE}/{bce}/nbb/{year}/{ref}.csv
   ex.  0878065378/nbb/2025/2026-00149705.csv
```

- `{bce}` = BCE 10 chiffres avec zéro de tête ;
- `{year}` = `periodEndDateYear` (année de clôture de l'exercice) ;
- `{ref}` = `reference` du dépôt (identifiant public, ex. `2026-00149705`).

### Deux State DB complémentaires

| Niveau | Collection | Clé | Statuts | Rôle |
|--------|------------|-----|---------|------|
| **Fichier** (Jour 1, réutilisé) | `file_state` (`STATE_COLLECTION`) | `(source, bce, kind, ref)` | `pending` → `done` \| `error` | Idempotence **fichier par fichier** : un CSV déjà `done` (ou déjà présent sur HDFS) n'est **jamais** re-téléchargé. |
| **Entreprise** (nouveau Jour 2) | `scrape_state` (`SCRAPE_STATE_COLLECTION`) | `bce` | `pending` → `in_progress` → `done` (+ `error`) | Avancement **par entreprise** : où en est le balayage d'une cible, combien de dépôts récupérés (`filings_count`). |

Cycle de vie côté entreprise :

```
        select_hotels()          début scrape            tous CSV OK
   (aucun) ─────────► pending ─────────────► in_progress ─────────► done  (filings_count=N)
                                                  │
                                                  │ échec réseau / HTTP
                                                  ▼
                                                error   (repris au prochain run)
```

### 429 & reprise (resume)

- **HTTP 429 / 5xx** : géré en amont par `consult_nbb._http_get` (délai poli
  `config.NBB_DELAY` avant chaque requête + **backoff exponentiel** respectant
  `Retry-After`, jusqu'à `config.HTTP_RETRIES` tentatives). `get_all_deposits`
  intègre déjà ce backoff sur la pagination.
- **Reprise après interruption** : si un run est coupé (429 persistant, crash,
  quota), l'entreprise reste `in_progress`/`pending` dans `scrape_state` et sera
  **re-sélectionnée** au run suivant. Les CSV **déjà téléchargés** sont
  **sautés** via `file_state.is_done()` (et le garde-fou `hdfs.exists`) : on ne
  reprend que le **delta** de dépôts manquants, sans jamais dupliquer une écriture
  HDFS. `scrape_state` passe à `done` uniquement lorsque tous les CSV attendus
  sont en `done`.

---

## 5. Nouveaux modules & fichiers (Jour 2)

| Fichier | Rôle |
|---------|------|
| `build_bronze.py` | Construit le **Bronze riche** `enterprise_finale` depuis les 7 CSV KBO (entreprise + dénominations/adresses/activités/contacts **joints sur `EntityNumber`**, + établissements/succursales **joints sur `EnterpriseNumber`**, **imbriqués bruts**). `contact`/`establishment`/`branch` sont optionnels (tableaux vides si absents du dump). Lecture en flux `dtype=str` (activity.csv = 1,5 Go → filtré, jamais chargé en entier). Upsert idempotent par `_id` = BCE 10 chiffres. |
| `codes.py` | Charge `code.csv` en index `(Category, Code) → libellé FR` ; expose `load_codes(path)`, `decode(category, code)`, et les helpers de décodage `status` / `juridical_form` / `type` / `nace` utilisés par Silver. |
| `silver.py` | Applique les **5 transformations** `enterprise_finale → enterprise_silver` (dates ISO, dédup activités, adresse REGO, dénomination 001 FR>NL, décodage FR). `build_silver(db, codes)`. |
| `hotel.py` | Ciblage hôtellerie : `HOTEL_NACE_CODES` (9 codes), `EXCLUDED_JURIDICAL_FORMS`, `select_hotels(db)` → liste des BCE ciblés (`status=AC`, `type=2`, activité `MAIN` dans les 9 codes, forme non exclue). |
| `scrape_nbb.py` | Scraping NBB **par entreprise** : liste des dépôts, filtre `>= config.YEAR_CSV_MIN`, download CSV → Bronze `{bce}/nbb/{year}/{ref}.csv`, double State DB (`file_state` + `scrape_state`), gestion 429 / reprise. |
| `dags/bce_silver_dag.py` | DAG Airflow `bce_silver` : `build_bronze` → `build_silver` (connexions Mongo créées **dans** les tâches ; instanciation `@dag` au niveau module). |
| `dags/bce_hotel_nbb_dag.py` | DAG Airflow `bce_hotel_nbb` : `select_hotels` (delta entreprise via `scrape_state`) → `scrape_one.expand(...)` (mapping dynamique par entreprise) → `report`. |

### Variables de configuration (Jour 2)

Ajoutées / utilisées dans `config.py` (toutes surchargeables par variable
d'environnement, comme au Jour 1) :

| Variable d'env | Constante | Défaut | Rôle |
|----------------|-----------|--------|------|
| `BCE_FINALE_COLLECTION` | `FINALE_COLLECTION` | `enterprise_finale` | Bronze riche (source de Silver) |
| `BCE_SILVER_COLLECTION` | `SILVER_COLLECTION` | `enterprise_silver` | Couche Silver nettoyée/enrichie |
| `BCE_SCRAPE_STATE_COLLECTION` | `SCRAPE_STATE_COLLECTION` | `scrape_state` | State DB **au niveau entreprise** |
| `BCE_KBO_ADDRESS_CSV` | `KBO_ADDRESS_CSV` | `/data/kbo/address.csv` | CSV adresses (build_bronze) |
| `BCE_KBO_ACTIVITY_CSV` | `KBO_ACTIVITY_CSV` | `/data/kbo/activity.csv` | CSV activités NACE (build_bronze) |
| `BCE_KBO_CODE_CSV` | `KBO_CODE_CSV` | `/data/kbo/code.csv` | CSV libellés (codes.py / Silver) |
| `BCE_YEAR_CSV_MIN` | `YEAR_CSV_MIN` | `2021` | Seuil `accountingYearEndDate` du scraping NBB |

---

## 6. Exécution

```bash
# (0) prérequis : CSV KBO sur disque/HDFS + Mongo joignable (ou mongomock en test)
export BCE_KBO_ENTERPRISE_CSV=/data/kbo/enterprise.csv
export BCE_KBO_DENOMINATION_CSV=/data/kbo/denomination.csv
export BCE_KBO_ADDRESS_CSV=/data/kbo/address.csv
export BCE_KBO_ACTIVITY_CSV=/data/kbo/activity.csv
export BCE_KBO_CODE_CSV=/data/kbo/code.csv

# (1) Bronze riche : enterprise_finale
python -m bce_ingestion.build_bronze

# (2) Silver : enterprise_silver (5 transforms)   — ou via le DAG bce_silver
python -m bce_ingestion.silver

# (3) Ciblage hôtellerie + scraping NBB (CSV >= 2021) vers Bronze HDFS
#     via le DAG Airflow bce_hotel_nbb (select_hotels -> scrape_one -> report)
airflow dags trigger bce_hotel_nbb
```

Ordre logique : **`build_bronze` → `build_silver` (ou DAG `bce_silver`) → DAG
`bce_hotel_nbb`**. Grâce aux deux State DB, chaque étape est **rejouable** et ne
retraite que le delta.

### Tests hors cluster

Comme au Jour 1 : `mongomock` pour Mongo (pymongo importé **paresseusement**),
`BCE_HDFS_BACKEND=local` pour écrire le Bronze dans un dossier local, et injection
systématique de `db` dans toutes les fonctions.
</content>
