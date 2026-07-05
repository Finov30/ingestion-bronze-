"""Source de données de l'explorateur — MongoDB réel OU jeu représentatif.

`get_source()` tente une connexion MongoDB courte ; si la base répond ET
contient des entreprises, on renvoie un `MongoSource`, sinon un `MockSource`
adossé à `sample_data`. Les deux exposent la même interface :

    .mode                      -> "mongo" | "demo"
    .search(q, sector, limit)  -> list[dict]  (fiches allégées)
    .get_company(bce)          -> dict | None (fiche complète + documents)
    .stats()                   -> dict (compteurs pour l'en-tête)

L'app web ne dépend donc jamais directement de MongoDB.
"""
from __future__ import annotations

import os
import re

from . import sample_data

# Config alignée sur bce_ingestion/config.py (surchargeable par env BCE_*).
MONGO_URI = os.environ.get("BCE_MONGO_URI", "mongodb://localhost:27017")
MONGO_DB = os.environ.get("BCE_MONGO_DB", "bce")
SILVER_COLLECTION = os.environ.get("BCE_SILVER_COLLECTION", "enterprise_silver")
COMPANIES_COLLECTION = os.environ.get("BCE_COMPANIES_COLLECTION", "companies")
STATE_COLLECTION = os.environ.get("BCE_STATE_COLLECTION", "file_state")

_DIGITS = re.compile(r"\D+")


def normalize_bce(value: str) -> str:
    """'0203.430.576' / '203430576' -> '0203430576' (10 chiffres, zéro de tête)."""
    digits = _DIGITS.sub("", value or "")
    return digits.zfill(10) if digits else ""


# Définition « hôtellerie » alignée sur hotel.py : is_hospitality persisté (si
# un jour écrit) OU activité NACE de préfixe 55, sur silver comme sur le repli.
HOSPITALITY_Q = {"$or": [
    {"is_hospitality": True},
    {"activities": {"$elemMatch": {"NaceCode": {"$regex": "^55"}}}},
    {"nace_code": {"$regex": "^55"}},
]}


def _region_from_zip(zipcode) -> str:
    """Région belge dérivée du code postal (référentiel bpost)."""
    try:
        z = int(str(zipcode)[:4])
    except (TypeError, ValueError):
        return ""
    if 1000 <= z <= 1299:
        return "Bruxelles-Capitale"
    if 1300 <= z <= 1499:
        return "Wallonie"
    if 1500 <= z <= 3999:
        return "Flandre"
    if 4000 <= z <= 7999:
        return "Wallonie"
    if 8000 <= z <= 9999:
        return "Flandre"
    return ""


def _first_denomination(doc: dict) -> str:
    """Première dénomination lisible d'un document silver (liste denominations)."""
    for d in (doc.get("denominations") or []):
        name = d.get("Denomination") or d.get("denomination")
        if name:
            return name
    return ""


def _lite(company: dict) -> dict:
    """Projection allégée d'une entreprise pour les résultats de recherche."""
    return {
        "bce": company["bce"],
        "bce_formatted": company["bce_formatted"],
        "denomination": company["denomination"],
        "status": company["status"],
        "status_label": company.get("status_label", company["status"]),
        "nace_code": company.get("nace_code", ""),
        "nace_label": company.get("nace_label", ""),
        "is_hospitality": company.get("is_hospitality", False),
        "region": company.get("region", ""),
        "city": company.get("city", ""),
        "doc_counts": company.get("doc_counts", {"total": 0}),
    }


# ---------------------------------------------------------------------------
# Source démo (jeu représentatif déterministe)
# ---------------------------------------------------------------------------
class MockSource:
    mode = "demo"

    def __init__(self) -> None:
        self._data = sample_data.dataset()

    def search(self, q: str, sector: str = "all", limit: int = 40) -> list[dict]:
        q = (q or "").strip()
        q_digits = _DIGITS.sub("", q)
        q_lower = q.lower()
        out: list[dict] = []
        for c in self._data.values():
            if sector == "hotel" and not c["is_hospitality"]:
                continue
            if q:
                by_bce = q_digits and q_digits in c["bce"]
                by_name = q_lower in c["denomination"].lower()
                if not (by_bce or by_name):
                    continue
            out.append(_lite(c))
        out.sort(key=lambda c: c["denomination"])
        return out[:limit]

    def get_company(self, bce: str) -> dict | None:
        return self._data.get(normalize_bce(bce))

    def stats(self) -> dict:
        vals = self._data.values()
        return {
            "companies": len(self._data),
            "hotels": sum(1 for c in vals if c["is_hospitality"]),
            "documents": sum(c["doc_counts"]["total"] for c in vals),
        }


# ---------------------------------------------------------------------------
# Source MongoDB réelle
# ---------------------------------------------------------------------------
class MongoSource:
    mode = "mongo"

    def __init__(self, db) -> None:
        self._db = db
        self._companies = db[SILVER_COLLECTION]
        self._fallback = db[COMPANIES_COLLECTION]
        self._state = db[STATE_COLLECTION]

    # -- mapping défensif d'un document Mongo (silver ou companies) --
    @staticmethod
    def _map_company(doc: dict) -> dict:
        bce = str(doc.get("_id") or doc.get("bce") or "").zfill(10)
        activities = doc.get("activities") or []
        main = next((a for a in activities if a.get("Classification") == "MAIN"), None)
        nace_code = (main or {}).get("NaceCode", doc.get("nace_code", "")) or ""
        addr = doc.get("address") or doc.get("seat") or {}
        status = doc.get("status") or doc.get("Status") or ""
        form = doc.get("juridical_form") or doc.get("JuridicalForm") or ""
        # silver : denomination_principale ; repli companies : denomination ;
        # sinon première entrée de la liste denominations.
        denomination = (doc.get("denomination_principale")
                        or doc.get("denomination")
                        or _first_denomination(doc)
                        or "(sans dénomination)")
        # silver : adresse REGO aux clés KBO brutes (MunicipalityFR, Zipcode…) ;
        # repli companies : clés minuscules éventuelles.
        zipcode = addr.get("Zipcode") or addr.get("zipcode") or ""
        city = (addr.get("MunicipalityFR") or addr.get("MunicipalityNL")
                or doc.get("city") or addr.get("municipality") or addr.get("city") or "")
        region = (doc.get("region") or addr.get("region")
                  or _region_from_zip(zipcode))
        return {
            "bce": bce,
            "bce_formatted": sample_data.format_bce(bce),
            "denomination": denomination,
            "status": status,
            "status_label": sample_data.STATUS_LABELS.get(status, status or "—"),
            "type": doc.get("type") or doc.get("TypeOfEnterprise", ""),
            "juridical_form": form,
            "form_label": sample_data.FORM_LABELS.get(form, form or "—"),
            "nace_code": nace_code,
            "nace_label": (sample_data.HOTEL_NACE.get(nace_code)
                           or sample_data.OTHER_NACE.get(nace_code)
                           or doc.get("nace_label", "")),
            "is_hospitality": bool(doc.get("is_hospitality",
                                   str(nace_code).startswith("55"))),
            "region": region,
            "city": city,
            "start_date": doc.get("start_date") or doc.get("StartDate", ""),
        }

    def _doc_counts_for(self, bces: list[str]) -> dict:
        """Compteurs de documents par entreprise en UNE agrégation (évite le N+1)."""
        base = {b: {"total": 0, **{s: 0 for s in sample_data.SOURCES}} for b in bces}
        if not bces:
            return base
        pipeline = [
            {"$match": {"bce": {"$in": bces}}},
            {"$group": {"_id": {"bce": "$bce", "source": "$source"},
                        "n": {"$sum": 1}}},
        ]
        for row in self._state.aggregate(pipeline):
            b, src, n = row["_id"]["bce"], row["_id"].get("source"), row["n"]
            entry = base.setdefault(b, {"total": 0, **{s: 0 for s in sample_data.SOURCES}})
            if src in entry:
                entry[src] += n
            entry["total"] += n
        return base

    def _documents_for(self, bce: str) -> list[dict]:
        docs: list[dict] = []
        for s in self._state.find({"bce": bce}):
            docs.append({
                "source": s.get("source", ""),
                "kind": s.get("kind", ""),
                "ref": s.get("ref", ""),
                "year": s.get("year", ""),
                "status": s.get("status", ""),
                "hdfs_path": s.get("hdfs_path", "—"),
                "size": s.get("size", 0),
                "title": s.get("title") or s.get("reference") or s.get("ref", ""),
                "date": s.get("date", ""),
                "financials": None,  # KPI calculés en aval (CSV non parsé ici)
            })
        return docs

    def search(self, q: str, sector: str = "all", limit: int = 40) -> list[dict]:
        q = (q or "").strip()[:100]  # borne la longueur (anti-DoS regex)
        clauses: list[dict] = []
        if sector == "hotel":
            clauses.append(HOSPITALITY_Q)
        if q:
            digits = _DIGITS.sub("", q)
            ors = [
                {"denomination_principale": {"$regex": re.escape(q), "$options": "i"}},
                {"denomination": {"$regex": re.escape(q), "$options": "i"}},
                {"denominations.Denomination": {"$regex": re.escape(q), "$options": "i"}},
            ]
            if digits:
                ors.append({"_id": {"$regex": "^" + re.escape(digits)}})  # ancré -> indexable
            clauses.append({"$or": ors})
        mongo_query = {"$and": clauses} if clauses else {}

        coll = self._companies
        if coll.estimated_document_count() == 0:
            coll = self._fallback
        # tri AVANT limit -> sélection déterministe, cohérente avec MockSource
        docs = list(coll.find(mongo_query)
                    .sort("denomination_principale", 1).limit(limit))
        companies = [self._map_company(d) for d in docs]
        counts = self._doc_counts_for([c["bce"] for c in companies])
        out = []
        for c in companies:
            c["doc_counts"] = counts.get(c["bce"], {"total": 0})
            out.append(_lite(c))
        out.sort(key=lambda c: c["denomination"])
        return out

    def get_company(self, bce: str) -> dict | None:
        bce = normalize_bce(bce)
        doc = self._companies.find_one({"_id": bce}) \
            or self._fallback.find_one({"_id": bce})
        if not doc:
            return None
        c = self._map_company(doc)
        c["documents"] = self._documents_for(bce)
        counts = {"total": len(c["documents"])}
        for src in sample_data.SOURCES:
            counts[src] = sum(1 for d in c["documents"] if d["source"] == src)
        c["doc_counts"] = counts
        return c

    def stats(self) -> dict:
        coll = self._companies
        if coll.estimated_document_count() == 0:
            coll = self._fallback
        return {
            "companies": coll.estimated_document_count(),
            "hotels": coll.count_documents(HOSPITALITY_Q),
            "documents": self._state.estimated_document_count(),
        }


# ---------------------------------------------------------------------------
# Fabrique
# ---------------------------------------------------------------------------
def get_source():
    """Renvoie une MongoSource si Mongo répond et contient des données, sinon MockSource."""
    try:
        import pymongo  # import paresseux (pile pyOpenSSL fragile en local)
        client = pymongo.MongoClient(MONGO_URI, serverSelectionTimeoutMS=1200)
        client.admin.command("ping")
        db = client[MONGO_DB]
        has_data = (db[SILVER_COLLECTION].estimated_document_count() > 0
                    or db[COMPANIES_COLLECTION].estimated_document_count() > 0)
        if has_data:
            return MongoSource(db)
    except Exception:
        pass
    return MockSource()
