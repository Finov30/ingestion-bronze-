"""Source NBB/CBSO — Central Balance Sheet Office (consult.cbso.nbb.be).

Module importable de la couche d'ingestion Bronze. Adapté du script de
référence du prof (``consult.py``) : on conserve ses pièces éprouvées
(``make_session``, ``parse_csv``, ``compute_kpis`` et les endpoints réels) mais
on supprime toute exécution au niveau module et toute écriture sur disque.

Les fonctions ``fetch_*`` renvoient le CONTENU (texte CSV / octets PDF) ; c'est
le pipeline (State DB + HDFS) qui décide du stockage.

NB : le portail NBB limite le débit. ``_http_get`` applique donc un délai poli
(``config.NBB_DELAY``) avant chaque requête et un backoff exponentiel sur les
réponses 429 / 5xx (jusqu'à ``config.HTTP_RETRIES`` tentatives).
"""
import time
from io import StringIO

import requests

from .. import config

BASE = "https://consult.cbso.nbb.be/api"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/149.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "sec-fetch-dest": "empty",
    "sec-fetch-mode": "cors",
    "sec-fetch-site": "same-origin",
}

# Taille de page utilisée pour paginer la liste des dépôts (le prof n'en
# récupérait que 10 ; on veut TOUT l'historique).
PAGE_SIZE = 50


# ---------------------------------------------------------------------------
# Session / HTTP
# ---------------------------------------------------------------------------
def make_session(enterprise_number: str) -> requests.Session:
    """Ouvre une session et établit les cookies (ASLBSA, ASLBSACORS, JSESSIONID).

    ``enterprise_number`` : numéro BCE (10 chiffres, avec zéro de tête).
    """
    session = requests.Session()
    session.headers.update(HEADERS)
    page_url = f"https://consult.cbso.nbb.be/consult-enterprise/{enterprise_number}"
    session.headers.update({"Referer": page_url})
    session.get(page_url)  # établit les cookies de session
    return session


def _http_get(
    session: requests.Session,
    url: str,
    *,
    delay: float | None = None,
    retries: int | None = None,
    timeout: float = 30.0,
    **kwargs,
) -> requests.Response:
    """GET poli avec backoff exponentiel sur 429 / 5xx.

    - Attend ``delay`` (défaut ``config.NBB_DELAY``) AVANT chaque requête.
    - Réessaie jusqu'à ``retries`` (défaut ``config.HTTP_RETRIES``) fois sur
      HTTP 429 ou 5xx, ainsi que sur les erreurs réseau, en doublant l'attente.
    - Respecte l'en-tête ``Retry-After`` si présent.
    Renvoie la réponse (sans ``raise_for_status`` — laissé à l'appelant) une
    fois un statut non réessayable obtenu.
    """
    if delay is None:
        delay = config.NBB_DELAY
    if retries is None:
        retries = config.HTTP_RETRIES

    backoff = max(delay, 0.5)
    last_exc: Exception | None = None

    for attempt in range(retries + 1):
        if delay:
            time.sleep(delay)
        try:
            resp = session.get(url, timeout=timeout, **kwargs)
        except requests.RequestException as exc:
            last_exc = exc
            if attempt >= retries:
                raise
            time.sleep(backoff)
            backoff *= 2
            continue

        if resp.status_code == 429 or 500 <= resp.status_code < 600:
            if attempt >= retries:
                return resp
            wait = backoff
            retry_after = resp.headers.get("Retry-After")
            if retry_after:
                try:
                    wait = max(wait, float(retry_after))
                except ValueError:
                    pass
            time.sleep(wait)
            backoff *= 2
            continue

        return resp

    # Ne devrait pas être atteint ; garde-fou.
    if last_exc is not None:
        raise last_exc
    raise RuntimeError(f"GET failed without response: {url}")


# ---------------------------------------------------------------------------
# Listing des dépôts (paginé)
# ---------------------------------------------------------------------------
def get_all_deposits(session: requests.Session, enterprise_number: str) -> list[dict]:
    """Récupère TOUS les dépôts publiés pour une entreprise, sur toutes les pages.

    Contrairement au ``get_deposits`` du prof (size=10, page unique), on pagine
    avec ``size=PAGE_SIZE`` trié ``periodEndDate,desc`` puis ``depositDate,desc``
    jusqu'à la dernière page. Chaque dépôt contient notamment : ``id``,
    ``periodEndDateYear``, ``enterpriseNumber``, ``reference``, ``migration``.
    """
    deposits: list[dict] = []
    page = 0
    while True:
        url = (
            f"{BASE}/rs-consult/published-deposits"
            f"?page={page}&size={PAGE_SIZE}"
            f"&enterpriseNumber={enterprise_number}"
            f"&sort=periodEndDate,desc&sort=depositDate,desc"
        )
        resp = _http_get(session, url)
        resp.raise_for_status()
        data = resp.json()

        content = data.get("content", [])
        deposits.extend(content)

        total_pages = data.get("totalPages")
        if total_pages is not None:
            if page >= total_pages - 1:
                break
        else:
            # Repli si l'API ne renvoie pas totalPages : on s'arrête sur une
            # page incomplète ou vide.
            if len(content) < PAGE_SIZE:
                break
        page += 1

    return deposits


# ---------------------------------------------------------------------------
# Récupération de contenu (CSV texte / PDF octets)
# ---------------------------------------------------------------------------
def fetch_csv_text(session: requests.Session, deposit_id: str) -> str:
    """Renvoie le texte CSV d'un dépôt (lève sur statut non-200)."""
    url = f"{BASE}/external/broker/public/deposits/consult/csv/{deposit_id}"
    resp = _http_get(session, url)
    resp.raise_for_status()
    return resp.text


def fetch_pdf_bytes(session: requests.Session, deposit: dict) -> bytes:
    """Renvoie les octets PDF d'un dépôt (lève sur statut non-200).

    ``deposit`` : un élément retourné par ``get_all_deposits`` (on lit son
    champ ``id``).
    """
    deposit_id = deposit["id"] if isinstance(deposit, dict) else deposit
    url = f"{BASE}/external/broker/public/deposits/pdf/{deposit_id}"
    resp = _http_get(session, url)
    resp.raise_for_status()
    return resp.content


# ---------------------------------------------------------------------------
# Parsing / KPIs (repris tels quels du prof)
# ---------------------------------------------------------------------------
def parse_csv(csv_text: str) -> dict:
    """Parse le CSV NBB (code -> valeur). Import pandas paresseux."""
    import pandas as pd

    df = pd.read_csv(StringIO(csv_text), header=None, skiprows=1)
    codes: dict = {}
    for _, row in df.iterrows():
        key = str(row[0]).strip()
        try:
            codes[key] = float(row[1])
        except (ValueError, TypeError):
            codes[key] = row[1]
    return codes


def compute_kpis(codes: dict) -> dict:
    def get(code):
        return codes.get(code, 0.0)

    omzet        = get("70")
    cogs         = get("60")
    depreciation = get("630")
    ebit         = get("9901")
    net_profit   = get("9904")
    cash         = get("54/58")
    equity       = get("10/15")
    total_assets = get("20/58")
    fin_debt     = get("17") + get("43")
    gross_profit = omzet - cogs
    ebitda       = ebit + depreciation

    def pct(num, denom):
        return round(num / denom * 100, 2) if denom else None

    return {
        "entity":           codes.get("Entity name"),
        "period_end":       codes.get("Accounting period end date"),
        "chiffre_affaires": omzet,
        "marge_brute":      gross_profit,
        "ebitda":           ebitda,
        "ebit":             ebit,
        "resultat_net":     net_profit,
        "taux_marge_brute": pct(gross_profit, omzet),
        "taux_ebitda":      pct(ebitda, omzet),
        "marge_nette":      pct(net_profit, omzet),
        "tresorerie":       cash,
        "dettes_fin":       fin_debt,
        "dette_nette":      fin_debt - cash,
        "fonds_propres":    equity,
        "total_actif":      total_assets,
        "autonomie_fin":    pct(equity, total_assets),
    }


# ---------------------------------------------------------------------------
# Démo (jamais exécutée à l'import)
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    demo_bce = "0878065378"
    sess = make_session(demo_bce)
    deps = get_all_deposits(sess, demo_bce)
    print(f"{len(deps)} deposits for {demo_bce}")
    if deps:
        print("deposit keys:", sorted(deps[0].keys()))
        d0 = deps[0]
        if not d0.get("migration"):
            txt = fetch_csv_text(sess, d0["id"])
            print("CSV head:", txt[:120].replace("\n", " | "))
        pdf = fetch_pdf_bytes(sess, d0)
        print("PDF starts with %PDF:", pdf[:4] == b"%PDF", "size:", len(pdf))
