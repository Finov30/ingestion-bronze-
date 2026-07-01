"""Scraper eJustice (Moniteur belge / publications d'entreprises).

Source : liste HTML des publications d'une entreprise sur le portail eJustice.
  https://www.ejustice.just.fgov.be/cgi_tsv/list.pl?language=fr&btw={digits}&page=N

Particularités du portail :
- Encodage ISO-8859-1 (latin-1) — jamais UTF-8.
- ~100 résultats max par page, chaque publication dans un ``div.list-item``.
- Pagination via ``&page=N`` (N commence à 1) jusqu'à ce qu'une page ne
  renvoie plus aucun élément.
- Le lien PDF est un ``<a href>`` contenant ``/tsv_pdf/`` ; l'URL complète est
  ``https://www.ejustice.just.fgov.be`` + href.

API publique :
  list_publications(bce_num10) -> list[dict]   # {date, numac, type, lien}
  fetch_pdf_bytes(url)         -> bytes         # PDF d'une publication
"""
from __future__ import annotations

import logging
import re
import time

import requests
from bs4 import BeautifulSoup

from .. import config

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constantes
# ---------------------------------------------------------------------------
DOMAIN   = "https://www.ejustice.just.fgov.be"
LIST_URL = DOMAIN + "/cgi_tsv/list.pl"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "fr-BE,fr;q=0.9,en;q=0.8",
}

# "2025-11-19 / 0146796" -> ("2025-11-19", "0146796")
DATE_NUMAC_RE = re.compile(r"(\d{4}-\d{2}-\d{2})\s*/\s*(\d+)")


# ---------------------------------------------------------------------------
# Session HTTP (avec retries polis)
# ---------------------------------------------------------------------------
def make_session() -> requests.Session:
    session = requests.Session()
    session.headers.update(HEADERS)
    return session


def _get(session: requests.Session, url: str, *, params: dict | None = None) -> requests.Response:
    """GET avec quelques tentatives (le portail rend parfois des 5xx/timeouts)."""
    last_exc: Exception | None = None
    for attempt in range(config.HTTP_RETRIES):
        try:
            r = session.get(url, params=params, timeout=30)
            r.raise_for_status()
            # le portail sert du latin-1, non déclaré de façon fiable
            r.encoding = "ISO-8859-1"
            return r
        except Exception as exc:  # noqa: BLE001
            last_exc = exc
            wait = config.EJUSTICE_DELAY * (attempt + 1)
            log.warning("eJustice GET échec (essai %d/%d) : %s", attempt + 1, config.HTTP_RETRIES, exc)
            time.sleep(wait)
    raise RuntimeError(f"eJustice GET a échoué après {config.HTTP_RETRIES} essais : {url}") from last_exc


# ---------------------------------------------------------------------------
# Parsing d'une page de résultats
# ---------------------------------------------------------------------------
def _parse_page(html: str) -> list[dict]:
    """Extrait les publications d'une page HTML (liste de dicts)."""
    soup = BeautifulSoup(html, "html.parser")
    publications: list[dict] = []

    for item in soup.find_all("div", class_="list-item"):
        # Le titre contient : adresse / n° BCE / TYPE / "DATE / NUMAC" / IMAGE
        title = item.find("a", class_="list-item--title")
        text = title.get_text("\n") if title else item.get_text("\n")
        lines = [ln.strip() for ln in text.split("\n") if ln.strip()]

        # date + numac via regex sur le texte complet de l'item
        m = DATE_NUMAC_RE.search(item.get_text(" ", strip=True))
        if not m:
            continue
        date, numac = m.group(1), m.group(2)

        # le type est la ligne juste avant "DATE / NUMAC"
        pub_type = None
        for idx, ln in enumerate(lines):
            if DATE_NUMAC_RE.search(ln):
                if idx > 0:
                    pub_type = lines[idx - 1]
                break

        # lien PDF : <a href> contenant /tsv_pdf/
        pdf = item.find("a", href=re.compile(r"/tsv_pdf/"))
        lien = DOMAIN + pdf["href"] if pdf and pdf.get("href") else None

        publications.append(
            {
                "date": date,
                "numac": numac,
                "type": pub_type,
                "lien": lien,
            }
        )

    return publications


# ---------------------------------------------------------------------------
# API publique
# ---------------------------------------------------------------------------
def list_publications(bce_num10: str, session: requests.Session | None = None) -> list[dict]:
    """Liste toutes les publications eJustice d'une entreprise.

    Args:
        bce_num10 : numéro BCE à 10 chiffres (avec le zéro de tête).
        session   : session requests optionnelle (réutilisation).

    Returns:
        Liste de dicts ``{date, numac, type, lien}`` (``lien`` = URL PDF complète
        ou ``None`` si la publication n'a pas d'image associée).
    """
    if session is None:
        session = make_session()

    # le portail attend les chiffres du numéro BCE (le zéro de tête est optionnel
    # côté serveur, mais on transmet la valeur telle quelle)
    digits = re.sub(r"\D", "", bce_num10)

    results: list[dict] = []
    page = 1
    while True:
        r = _get(session, LIST_URL, params={"language": "fr", "btw": digits, "page": page})
        batch = _parse_page(r.text)
        if not batch:
            break
        results.extend(batch)
        log.info("[%s] page %d — %d publications (total %d)", bce_num10, page, len(batch), len(results))
        page += 1
        time.sleep(config.EJUSTICE_DELAY)

    return results


def fetch_pdf_bytes(url: str, session: requests.Session | None = None) -> bytes:
    """Télécharge le PDF d'une publication et renvoie ses octets bruts."""
    if session is None:
        session = make_session()
    time.sleep(config.EJUSTICE_DELAY)
    r = _get(session, url)
    return r.content
