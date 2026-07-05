"""Application FastAPI — backend Jour 3 (consultation entreprises belges).

Monte les routers (search / enterprise / dirigeants / statutes) et configure
CORS pour l'origine du serveur de dev Vite (http://localhost:5173) ainsi que
``*`` pour simplifier le développement.
"""
from __future__ import annotations

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from .routers import dirigeants, enterprise, search, statutes

app = FastAPI(
    title="BCE Consultation API",
    version="1.0.0",
    description="Silver (identité) + Gold (finances) + scraping dirigeants/statuts.",
)

# CORS : Vite dev + wildcard (simplicité). allow_credentials=False car "*".
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173", "*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/api/health")
def health() -> dict:
    return {"status": "ok"}


app.include_router(search.router)
app.include_router(enterprise.router)
app.include_router(dirigeants.router)
app.include_router(statutes.router)
