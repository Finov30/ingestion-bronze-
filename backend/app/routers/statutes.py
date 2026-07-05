"""GET /api/enterprise/{bce}/statutes/stream — statuts notariés en SSE.

- Cache présent (collection Mongo ``statutes``) : on streame instantanément les
  statuts mémorisés.
- Sinon : on gratte notaire.be via ``strapor_notaire.iter_statutes`` — un
  GÉNÉRATEUR qui yield chaque statut au fil de la pagination — exécuté dans un
  thread producteur ; chaque document est émis DÈS son arrivée (streaming réel,
  pas d'attente de fin de scrape), puis l'ensemble est persisté en cache.
- Échec du scraper : ``event: error`` puis fermeture propre (jamais de 500).

Format SSE (contrat) :
    data: {json d'un statut}\n\n            (un statut, au fil de l'eau)
    event: done\ndata: {"count": N}\n\n     (fin)
"""
from __future__ import annotations

import datetime as _dt

from fastapi import APIRouter, Depends
from fastapi.responses import StreamingResponse

from ..db import STATUTES_COLLECTION, get_db
from ..sse import SSE_HEADERS, num10, sse_data, sse_event, stream_from_generator

router = APIRouter(prefix="/api", tags=["statutes"])


def _map_statute(raw: dict) -> dict:
    """Projette un statut brut strapor vers la forme du contrat frontend."""
    return {
        "document": raw.get("documentTitle") or raw.get("document"),
        "date": raw.get("deedDate") or raw.get("date"),
        "notaire": raw.get("organizationName") or raw.get("notaire"),
        "statut": raw.get("documentStatus") or raw.get("statut"),
        "documentId": raw.get("documentId"),
    }


@router.get("/enterprise/{bce}/statutes/stream")
async def stream_statutes(bce: str, db=Depends(get_db)) -> StreamingResponse:
    coll = db[STATUTES_COLLECTION]
    cached = coll.find_one({"_id": bce})

    async def gen():
        # 1) Cache présent -> streamer instantanément.
        if cached is not None:
            statutes = cached.get("statutes", []) or []
            for s in statutes:
                yield sse_data(s)
            yield sse_event("done", {"count": len(statutes), "cached": True})
            return

        # 2) Live : le générateur bloquant iter_statutes tourne dans un thread ;
        #    on émet chaque statut DÈS réception (progressif, pas d'attente).
        def _make_gen():
            from bce_ingestion.sources import strapor_notaire
            return strapor_notaire.iter_statutes(num10(bce))

        mapped: list[dict] = []
        async for kind, payload in stream_from_generator(_make_gen, map_item=_map_statute):
            if kind == "item":
                mapped.append(payload)
                yield sse_data(payload)
            elif kind == "error":
                yield sse_event("error", {"message": payload, "enterprise_number": bce})
                return

        # 3) Persister le tout dans le cache (best-effort) puis signaler la fin.
        try:
            coll.update_one(
                {"_id": bce},
                {"$set": {"statutes": mapped, "last_updated": _dt.datetime.utcnow()}},
                upsert=True,
            )
        except Exception:  # pragma: no cover - persistance best-effort
            pass

        yield sse_event("done", {"count": len(mapped), "cached": False})

    return StreamingResponse(gen(), media_type="text/event-stream", headers=SSE_HEADERS)
