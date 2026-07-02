"""GET /api/enterprise/{bce} — fiche complète Silver + Gold."""
from __future__ import annotations

from fastapi import APIRouter, Depends

from ..db import GOLD_COLLECTION, SILVER_COLLECTION, get_db

router = APIRouter(prefix="/api", tags=["enterprise"])


def _silver_view(doc: dict | None) -> dict | None:
    if not doc:
        return None
    addr = doc.get("address") or None
    address = None
    if isinstance(addr, dict):
        address = {
            "StreetFR": addr.get("StreetFR"),
            "HouseNumber": addr.get("HouseNumber"),
            "Zipcode": addr.get("Zipcode"),
            "MunicipalityFR": addr.get("MunicipalityFR"),
        }
    activities = []
    for a in doc.get("activities") or []:
        activities.append(
            {
                "NaceCode": a.get("NaceCode"),
                "Classification": a.get("Classification"),
                "NaceLabel": a.get("NaceLabel"),
            }
        )
    return {
        "denomination_principale": doc.get("denomination_principale"),
        "StatusLabel": doc.get("StatusLabel"),
        "JuridicalFormLabel": doc.get("JuridicalFormLabel"),
        "StartDate": doc.get("StartDate"),
        "address": address,
        "activities": activities,
    }


def _ratios_view(r: dict | None) -> dict:
    r = r or {}
    return {
        "marge_nette": r.get("marge_nette"),
        "roe": r.get("roe"),
        "ratio_liquidite": r.get("ratio_liquidite"),
        "taux_endettement": r.get("taux_endettement"),
    }


def _year_view(y: dict) -> dict:
    return {
        "year": y.get("year"),
        "ca": y.get("ca"),
        "marge_brute": y.get("marge_brute"),
        "ebit": y.get("ebit"),
        "resultat_net": y.get("resultat_net"),
        "tresorerie": y.get("tresorerie"),
        "dettes_financieres": y.get("dettes_financieres"),
        "fonds_propres": y.get("fonds_propres"),
        "capital_souscrit": y.get("capital_souscrit"),
        "ratios": _ratios_view(y.get("ratios")),
    }


def _gold_view(doc: dict | None) -> dict | None:
    if not doc:
        return None
    years = [_year_view(y) for y in (doc.get("years") or [])]
    return {
        "schema_type": doc.get("schema_type"),
        "last_updated": _isoformat(doc.get("last_updated")),
        "years": years,
    }


def _isoformat(value):
    """Sérialise proprement un datetime éventuel (last_updated)."""
    try:
        import datetime as _dt

        if isinstance(value, _dt.datetime):
            return value.isoformat()
    except Exception:  # pragma: no cover
        pass
    return value


@router.get("/enterprise/{bce}")
def get_enterprise(bce: str, db=Depends(get_db)) -> dict:
    silver = db[SILVER_COLLECTION].find_one({"_id": bce})
    gold = db[GOLD_COLLECTION].find_one({"enterprise_number": bce})
    if gold is None:
        # Repli : certains pipelines utilisent _id = bce pour le gold aussi.
        gold = db[GOLD_COLLECTION].find_one({"_id": bce})
    return {
        "enterprise_number": bce,
        "silver": _silver_view(silver),
        "gold": _gold_view(gold),
    }
