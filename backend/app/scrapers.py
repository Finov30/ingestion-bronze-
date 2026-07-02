"""Scrapers à la demande utilisés par le backend.

- ``scrape_dirigeants`` : gratte la fiche publique kbopub d'une entreprise et
  renvoie ``[{"nom": str, "qualites": [str]}]``. Bloquant (requests+bs4) : les
  routers l'exécutent dans un thread pour ne pas bloquer la boucle asyncio.

Proxies Tor : optionnels et DÉSACTIVÉS par défaut. Renseigner la variable
d'environnement ``BCE_TOR_PROXIES`` (ex. ``socks5h://127.0.0.1:9050``) pour les
activer ; sinon aucun proxy n'est utilisé.
"""
from __future__ import annotations

import os
import re

KBOPUB_URL = (
    "https://kbopub.economie.fgov.be/kbopub/toonondernemingps.html"
    "?ondernemingsnummer={num}&lang=fr"
)

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36"
)


def _num_short(bce: str) -> str:
    """Numéro BCE en chiffres, sans zéro de tête (format attendu par kbopub)."""
    digits = re.sub(r"\D", "", bce or "")
    return digits.lstrip("0") or digits


def _proxies():
    """Proxies requests optionnels (Tor). Désactivés par défaut."""
    tor = os.environ.get("BCE_TOR_PROXIES", "").strip()
    if not tor:
        return None
    return {"http": tor, "https": tor}


def scrape_dirigeants(bce: str, *, timeout: float = 20.0) -> list[dict]:
    """Gratte la liste des dirigeants (fonctions) depuis kbopub.

    Renvoie ``[{"nom": str, "qualites": [str]}]``. En cas d'erreur réseau /
    parsing, renvoie une liste vide (l'appelant décide quoi persister).
    """
    import requests  # imports locaux : discipline paresseuse + tests légers
    from bs4 import BeautifulSoup

    num = _num_short(bce)
    url = KBOPUB_URL.format(num=num)
    headers = {
        "User-Agent": USER_AGENT,
        "Accept-Language": "fr-BE,fr;q=0.9,en;q=0.8",
    }
    resp = requests.get(url, headers=headers, proxies=_proxies(), timeout=timeout)
    resp.raise_for_status()
    return parse_dirigeants_html(resp.text)


def parse_dirigeants_html(html: str) -> list[dict]:
    """Extrait les dirigeants de la page kbopub (séparé pour être testable).

    kbopub présente les fonctions dans un tableau dont la première colonne est
    la qualité (ex. « Administrateur ») et la seconde le nom de la personne.
    La section est repérée par l'ancre / l'intitulé « Fonctions ». On agrège
    par personne (un même nom peut cumuler plusieurs qualités).
    """
    from bs4 import BeautifulSoup

    soup = BeautifulSoup(html, "html.parser")

    # Localiser la ligne d'en-tête « Fonctions » puis lire les <tr> qui suivent.
    people: dict[str, dict] = {}
    order: list[str] = []

    def _add(nom: str, qualite: str) -> None:
        nom = _clean(nom)
        qualite = _clean(qualite)
        if not nom or not qualite:
            return
        if nom not in people:
            people[nom] = {"nom": nom, "qualites": []}
            order.append(nom)
        if qualite not in people[nom]["qualites"]:
            people[nom]["qualites"].append(qualite)

    # Stratégie : trouver l'élément contenant le libellé "Fonctions", remonter à
    # son tableau, puis parcourir les lignes qualité/nom.
    anchor = soup.find(
        lambda tag: tag.name in ("td", "th", "h2", "div", "span")
        and tag.get_text(strip=True).lower().startswith("fonction")
    )
    tables = []
    if anchor is not None:
        tbl = anchor.find_parent("table")
        if tbl is not None:
            tables.append(tbl)
    if not tables:
        tables = soup.find_all("table")

    for tbl in tables:
        for tr in tbl.find_all("tr"):
            cells = tr.find_all("td")
            if len(cells) < 2:
                continue
            qualite = cells[0].get_text(" ", strip=True)
            nom = cells[1].get_text(" ", strip=True)
            # Filtrer les lignes hors-fonctions (heuristique : la qualité
            # ressemble à un rôle, pas à une date ou un numéro).
            if _looks_like_role(qualite) and nom:
                _add(nom, qualite)

    return [people[n] for n in order]


_ROLE_HINTS = (
    "administrateur",
    "gérant",
    "gerant",
    "représentant",
    "representant",
    "délégué",
    "delegue",
    "commissaire",
    "liquidateur",
    "président",
    "president",
    "directeur",
    "fondé",
    "fonde",
    "mandataire",
    "associé",
    "associe",
)


def _looks_like_role(text: str) -> bool:
    t = (text or "").lower()
    return any(h in t for h in _ROLE_HINTS)


def _clean(text: str) -> str:
    return re.sub(r"\s+", " ", (text or "").replace("\xa0", " ")).strip()
