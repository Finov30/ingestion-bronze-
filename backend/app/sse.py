"""Helpers Server-Sent Events (SSE) partagés par les routeurs statuts/dirigeants.

Un flux SSE émet des lignes ``data: ...\\n\\n`` (message par défaut) et des
événements nommés ``event: <name>\\ndata: ...\\n\\n`` (ici ``done`` / ``error``).

Le pattern de STREAMING PROGRESSIF depuis un scraper *bloquant* (Playwright /
requests) : un thread producteur consomme le générateur et pousse chaque item
dans une ``asyncio.Queue`` via ``loop.call_soon_threadsafe`` ; le générateur
asynchrone de la ``StreamingResponse`` lit la queue et émet chaque item DÈS son
arrivée — sans attendre la fin du scrape. Voir :func:`stream_from_generator`.
"""
from __future__ import annotations

import asyncio
import json
import threading


def sse_data(obj: dict) -> str:
    return f"data: {json.dumps(obj, ensure_ascii=False)}\n\n"


def sse_event(event: str, obj: dict) -> str:
    return f"event: {event}\ndata: {json.dumps(obj, ensure_ascii=False)}\n\n"


def num10(bce: str) -> str:
    """Numéro BCE normalisé en 10 chiffres avec zéro de tête."""
    digits = "".join(ch for ch in (bce or "") if ch.isdigit())
    return digits.zfill(10) if digits else digits


SSE_HEADERS = {
    "Cache-Control": "no-cache",
    "Connection": "keep-alive",
    "X-Accel-Buffering": "no",  # désactive le buffering nginx -> vraiment progressif
}


async def stream_from_generator(make_generator, map_item=None):
    """Async-générateur SSE alimenté par un générateur SYNCHRONE bloquant.

    Args:
        make_generator : callable sans argument renvoyant un itérateur (le
                         scraper, ex. ``lambda: strapor_notaire.iter_statutes(num)``).
        map_item       : projection optionnelle appliquée à chaque item brut.

    Émet un ``data:`` par item (au fil de l'eau), puis on rend la main à
    l'appelant qui décide de persister + émettre ``event: done`` (l'appelant
    reçoit la liste des items mappés via la ``StopAsyncIteration`` — ici on
    renvoie plutôt les items au fur et à mesure et l'appelant agrège).
    """
    loop = asyncio.get_running_loop()
    queue: asyncio.Queue = asyncio.Queue()
    _SENTINEL = object()

    def _producer():
        try:
            for raw in make_generator():
                loop.call_soon_threadsafe(queue.put_nowait, ("item", raw))
        except Exception as exc:  # noqa: BLE001 - remonté au flux
            loop.call_soon_threadsafe(queue.put_nowait, ("error", str(exc)))
        finally:
            loop.call_soon_threadsafe(queue.put_nowait, ("end", _SENTINEL))

    threading.Thread(target=_producer, daemon=True).start()

    while True:
        kind, payload = await queue.get()
        if kind == "item":
            item = map_item(payload) if map_item else payload
            yield ("item", item)
        elif kind == "error":
            yield ("error", payload)
            return
        else:  # end
            return
