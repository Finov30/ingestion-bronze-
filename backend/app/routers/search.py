"""GET /api/search — recherche d'entreprises dans la couche Silver."""
from __future__ import annotations

import re

from fastapi import APIRouter, Depends, Query

from ..db import SILVER_COLLECTION, get_db

router = APIRouter(prefix="/api", tags=["search"])


def _result(doc: dict) -> dict:
    return {
        "enterprise_number": doc.get("_id"),
        "denomination": doc.get("denomination_principale"),
        "status_label": doc.get("StatusLabel"),
        "juridical_form_label": doc.get("JuridicalFormLabel"),
    }


@router.get("/search")
def search(
    q: str = Query("", description="Nom ou numéro BCE"),
    limit: int = Query(20, ge=1, le=100),
    db=Depends(get_db),
) -> dict:
    """Recherche par numéro (préfixe de ``_id``) ou par nom (regex insensible).

    - ``q`` composé uniquement de chiffres → match préfixe sur ``_id``.
    - sinon → regex insensible à la casse sur ``denomination_principale`` et le
      tableau ``denominations``.
    """
    term = (q or "").strip()
    coll = db[SILVER_COLLECTION]
    if not term:
        return {"results": []}

    digits = re.sub(r"\D", "", term)
    if term.replace(" ", "").isdigit() and digits:
        # Recherche par préfixe de numéro d'entreprise.
        query = {"_id": {"$regex": "^" + re.escape(digits)}}
    else:
        rx = {"$regex": re.escape(term), "$options": "i"}
        query = {
            "$or": [
                {"denomination_principale": rx},
                {"denominations": rx},
            ]
        }

    cursor = coll.find(query).limit(int(limit))
    return {"results": [_result(d) for d in cursor]}
