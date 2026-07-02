"""Dirigeants (kbopub) — endpoint JSON classique + endpoint SSE.

- GET /api/enterprise/{bce}/dirigeants          -> {"dirigeants":[...], "cached":bool}
- GET /api/enterprise/{bce}/dirigeants/stream   -> SSE (un dirigeant par frame)

Les deux servent depuis le cache Mongo ``dirigeants`` si présent, sinon grattent
kbopub une seule fois puis persistent (pas de re-scrape). Le stream SSE est la
forme attendue par l'énoncé (« Dirigeants … scrape SSE, persiste en base »).
"""
from __future__ import annotations

import datetime as _dt

from fastapi import APIRouter, Depends
from fastapi.responses import StreamingResponse
from starlette.concurrency import run_in_threadpool

from ..db import DIRIGEANTS_COLLECTION, get_db
from ..sse import SSE_HEADERS, sse_data, sse_event
from .. import scrapers

router = APIRouter(prefix="/api", tags=["dirigeants"])


def _persist(coll, bce: str, dirigeants: list) -> None:
    try:
        coll.update_one(
            {"_id": bce},
            {"$set": {"dirigeants": dirigeants, "last_updated": _dt.datetime.utcnow()}},
            upsert=True,
        )
    except Exception:  # pragma: no cover - persistance best-effort
        pass


@router.get("/enterprise/{bce}/dirigeants")
async def get_dirigeants(bce: str, db=Depends(get_db)) -> dict:
    """Sert depuis le cache Mongo ``dirigeants`` si présent, sinon gratte
    kbopub (dans un thread), persiste puis renvoie.

    Réponse : ``{"dirigeants": [{"nom", "qualites": [...]}], "cached": bool}``.
    """
    coll = db[DIRIGEANTS_COLLECTION]
    cached = coll.find_one({"_id": bce})
    if cached is not None:
        return {"dirigeants": cached.get("dirigeants", []), "cached": True}

    try:
        dirigeants = await run_in_threadpool(scrapers.scrape_dirigeants, bce)
    except Exception as exc:  # réseau/parsing : ne pas planter l'API
        return {"dirigeants": [], "cached": False, "error": str(exc)}

    _persist(coll, bce, dirigeants)
    return {"dirigeants": dirigeants, "cached": False}


@router.get("/enterprise/{bce}/dirigeants/stream")
async def stream_dirigeants(bce: str, db=Depends(get_db)) -> StreamingResponse:
    """Diffuse les dirigeants en SSE : un ``data:`` par dirigeant puis
    ``event: done``. Sert le cache instantanément, sinon gratte kbopub une fois
    (dans un thread), streame chaque dirigeant et persiste."""
    coll = db[DIRIGEANTS_COLLECTION]
    cached = coll.find_one({"_id": bce})

    async def gen():
        # 1) Cache -> streamer instantanément.
        if cached is not None:
            dirigeants = cached.get("dirigeants", []) or []
            for d in dirigeants:
                yield sse_data(d)
            yield sse_event("done", {"count": len(dirigeants), "cached": True})
            return

        # 2) Scrape kbopub (un seul appel bloquant -> thread), puis stream + persist.
        try:
            dirigeants = await run_in_threadpool(scrapers.scrape_dirigeants, bce)
        except Exception as exc:
            yield sse_event("error", {"message": str(exc), "enterprise_number": bce})
            return

        for d in dirigeants or []:
            yield sse_data(d)

        _persist(coll, bce, dirigeants or [])
        yield sse_event("done", {"count": len(dirigeants or []), "cached": False})

    return StreamingResponse(gen(), media_type="text/event-stream", headers=SSE_HEADERS)
