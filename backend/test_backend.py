"""Validation du backend FastAPI avec TestClient + mongomock."""
import sys
import datetime

sys.path.insert(0, "/mnt/c/Users/HDCC5629/Downloads/backend")

import mongomock
from fastapi.testclient import TestClient

from app.main import app
from app.db import (
    get_db,
    SILVER_COLLECTION,
    GOLD_COLLECTION,
    DIRIGEANTS_COLLECTION,
    STATUTES_COLLECTION,
)

BCE = "0878065378"

# --- base mongomock pré-remplie -------------------------------------------
mdb = mongomock.MongoClient().db

mdb[SILVER_COLLECTION].insert_one({
    "_id": BCE,
    "denomination_principale": "GOOGLE BELGIUM",
    "denominations": ["GOOGLE BELGIUM"],
    "StatusLabel": "Active",
    "JuridicalFormLabel": "Société privée à responsabilité limitée",
    "StartDate": "2010-05-01",
    "address": {
        "TypeOfAddress": "REGO",
        "StreetFR": "Rue de la Loi",
        "HouseNumber": "1",
        "Zipcode": "1000",
        "MunicipalityFR": "Bruxelles",
    },
    "activities": [
        {"NaceCode": "55100", "Classification": "MAIN", "NaceLabel": "Hôtels"}
    ],
})

mdb[GOLD_COLLECTION].insert_one({
    "_id": BCE,
    "enterprise_number": BCE,
    "schema_type": "abrege",
    "last_updated": datetime.datetime(2026, 7, 1, 12, 0, 0),
    "years": [
        {
            "year": 2024, "ca": 1000.0, "marge_brute": 400.0, "ebit": 200.0,
            "resultat_net": 150.0, "tresorerie": 300.0, "dettes_financieres": 100.0,
            "fonds_propres": 500.0, "capital_souscrit": 50.0,
            "ratios": {"marge_nette": 15.0, "roe": 30.0,
                       "ratio_liquidite": 3.0, "taux_endettement": 20.0},
        }
    ],
})

# cache dirigeants (sert depuis le cache -> pas de réseau)
mdb[DIRIGEANTS_COLLECTION].insert_one({
    "_id": BCE,
    "dirigeants": [{"nom": "Jean Dupont", "qualites": ["Administrateur"]}],
    "last_updated": datetime.datetime.utcnow(),
})

# cache statuts (stream instantané)
mdb[STATUTES_COLLECTION].insert_one({
    "_id": BCE,
    "statutes": [{
        "document": "Acte constitutif", "date": "2010-05-01",
        "notaire": "Me Notaire", "statut": "DONE", "documentId": "abc123",
    }],
    "last_updated": datetime.datetime.utcnow(),
})

app.dependency_overrides[get_db] = lambda: mdb
client = TestClient(app)

passed = []


def check(name, cond):
    passed.append((name, bool(cond)))
    print(("PASS" if cond else "FAIL"), name)


# 1) health
r = client.get("/api/health")
check("health ok", r.status_code == 200 and r.json() == {"status": "ok"})

# 2) search by name
r = client.get("/api/search", params={"q": "goog"})
res = r.json().get("results", [])
check("search?q=goog returns seeded company",
      r.status_code == 200 and any(x["enterprise_number"] == BCE for x in res))
check("search result shape",
      res and set(res[0].keys()) == {"enterprise_number", "denomination",
                                     "status_label", "juridical_form_label"})

# 2b) search by number prefix
r = client.get("/api/search", params={"q": "0878"})
check("search by number prefix",
      any(x["enterprise_number"] == BCE for x in r.json()["results"]))

# 3) enterprise silver+gold
r = client.get(f"/api/enterprise/{BCE}")
j = r.json()
check("enterprise returns silver",
      j["silver"] and j["silver"]["denomination_principale"] == "GOOGLE BELGIUM"
      and j["silver"]["address"]["MunicipalityFR"] == "Bruxelles"
      and j["silver"]["activities"][0]["NaceCode"] == "55100")
check("enterprise returns gold",
      j["gold"] and j["gold"]["schema_type"] == "abrege"
      and j["gold"]["years"][0]["ca"] == 1000.0
      and j["gold"]["years"][0]["ratios"]["roe"] == 30.0)
check("enterprise_number field", j["enterprise_number"] == BCE)

# 3b) unknown enterprise -> nulls, no crash
r = client.get("/api/enterprise/9999999999")
jj = r.json()
check("unknown enterprise nulls",
      r.status_code == 200 and jj["silver"] is None and jj["gold"] is None)

# 4) dirigeants from cache (endpoint JSON classique)
r = client.get(f"/api/enterprise/{BCE}/dirigeants")
j = r.json()
check("dirigeants from cache",
      r.status_code == 200 and isinstance(j, dict) and j["cached"] is True
      and j["dirigeants"][0]["nom"] == "Jean Dupont")

# 4b) dirigeants en SSE (F4) — cache -> une frame par dirigeant + done
with client.stream("GET", f"/api/enterprise/{BCE}/dirigeants/stream") as sd:
    dbody = "".join(chunk for chunk in sd.iter_text())
check("dirigeants SSE (F4)",
      "text/event-stream" in sd.headers.get("content-type", "")
      and '"nom": "Jean Dupont"' in dbody and "event: done" in dbody)

# 5) SSE statutes from cache
with client.stream("GET", f"/api/enterprise/{BCE}/statutes/stream") as s:
    body = "".join(chunk for chunk in s.iter_text())
check("SSE content-type",
      "text/event-stream" in s.headers.get("content-type", ""))
check("SSE yields cached statute", '"documentId": "abc123"' in body
      and '"document": "Acte constitutif"' in body)
check("SSE done event", "event: done" in body and '"count": 1' in body)

print("\n--- SSE body ---")
print(body)

n_pass = sum(1 for _, ok in passed if ok)
print(f"\nRESULT: {n_pass}/{len(passed)} checks passed")
sys.exit(0 if n_pass == len(passed) else 1)
