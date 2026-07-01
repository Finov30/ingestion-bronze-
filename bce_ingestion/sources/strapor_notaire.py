"""Source notaire.be (statuts / actes constitutifs) — couche Bronze.

Adapté du scraper du prof (``strapor.py``). Le portail
``statuts.notaire.be`` est protégé par un mur anti-bot F5/Shape : un simple
``requests`` reçoit une page de challenge JavaScript au lieu du JSON.

Méthode qui fonctionne (validée dans cet environnement) sur un serveur SANS
écran :

1. Lancer Playwright chromium avec le **nouveau moteur headless** de Chromium
   (``--headless=new``) quand ``config.NOTAIRE_HEADLESS`` est vrai, plus
   ``--no-sandbox`` et ``--disable-blink-features=AutomationControlled``.
   IMPORTANT : l'ANCIEN moteur headless (``launch(headless=True)``) est détecté
   et bloqué par le challenge F5 (le cookie ``Lyp1CWKh`` n'est jamais émis) ;
   le nouveau moteur headless, lui, résout le challenge exactement comme un
   Chrome visible. Si un écran est disponible (``NOTAIRE_HEADLESS`` faux), on
   lance carrément en mode visible.
2. ``goto`` la page front-end
   ``/stapor_v1/enterprise/{numShort}/statutes`` en ``wait_until='domcontentloaded'``
   (surtout PAS ``networkidle`` — la page ne devient jamais idle). Le navigateur
   exécute le challenge JS F5 et pose les cookies (``OClmoOot`` + ``Lyp1CWKh``).
3. **Poller** l'API JSON via ``ctx.request.get`` (qui porte ces cookies) jusqu'à
   recevoir du JSON :
   ``/stapor_v1/api/enterprises/{num10}/statutes?deedDate=&offset=&limit=``.
   ``num10`` = numéro BCE 10 chiffres AVEC le zéro de tête (un numéro à 9
   chiffres renvoie HTTP 400). ``numShort`` = ``num10.lstrip('0')``.
   Le portail applique aussi un rate-limit (HTTP 429 ``TooManyRequests``) : on
   temporise plus longuement sur un 429.

Le contexte Playwright porte les cookies F5, donc le PDF par statut est
téléchargé via le MÊME ``ctx.request`` :
``/stapor_v1/api/enterprises/{num10}/statutes/non-certified/{documentId}``.

Comme ``sync_playwright`` refuse de tourner à l'intérieur d'une boucle asyncio
(Jupyter / Airflow), toute utilisation de Playwright est encapsulée dans un
thread dédié (``_run_in_thread``) : le module marche donc aussi bien en script
standalone que dans un worker asyncio.

API publique :
    get_session() -> _NotaireSession
    get_statutes(bce_num10) -> list[dict]
    fetch_statute_pdf_bytes(bce_num10, statute) -> bytes | None
    needs_notaire_check(forme_juridique, status='Active') -> bool
"""
from __future__ import annotations

import json
import logging
import threading
import time
from pathlib import Path

from .. import config

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constantes (reprises du fichier du prof)
# ---------------------------------------------------------------------------
BASE = "https://statuts.notaire.be/stapor_v1"
COOKIE_FILE = Path("notaire_cookies.json")
PAGE_SIZE = 20

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36"
)

# Formes juridiques qui n'ont pas de statut chez le notaire (personnes
# physiques, entités publiques, etc.) — reprises verbatim du prof.
NO_NOTAIRE_FORMS = {"009", "017", "018", "025", "026", "027", "051", "052"}

# Nombre de tentatives de polling de l'API JSON après le goto front-end,
# le temps que le challenge F5 se résolve.
POLL_ATTEMPTS = 30
POLL_INTERVAL_MS = 1500
# Attente prolongée quand le portail répond 429 (TooManyRequests).
RATE_LIMIT_WAIT_MS = 5000
# Tentatives pour le téléchargement PDF : l'endpoint non-certifié re-challenge
# parfois (ctx.request n'exécute pas le JS), il faut re-goto puis réessayer.
PDF_ATTEMPTS = 6

# Args de lancement chromium prouvés dans cet environnement.
# --headless=new : nouveau moteur headless (passe le challenge F5, sans écran).
BASE_ARGS = ["--no-sandbox", "--disable-blink-features=AutomationControlled"]
NEW_HEADLESS_ARG = "--headless=new"


# ---------------------------------------------------------------------------
# Helpers numéro BCE
# ---------------------------------------------------------------------------
def _num10(bce: str) -> str:
    """Normalise en 10 chiffres AVEC zéro de tête (l'API 9 chiffres -> 400)."""
    digits = "".join(ch for ch in str(bce) if ch.isdigit())
    return digits.zfill(10)


def _num_short(num10: str) -> str:
    """Numéro sans les zéros de tête, pour l'URL front-end."""
    return num10.lstrip("0")


# ---------------------------------------------------------------------------
# Exécution de Playwright dans un thread (compatible boucle asyncio)
# ---------------------------------------------------------------------------
def _run_in_thread(fn, *args, **kwargs):
    """Exécute ``fn`` dans un thread dédié et renvoie son résultat.

    ``sync_playwright`` lève ``It looks like you are using Playwright Sync API
    inside the asyncio loop`` si on l'appelle depuis une boucle asyncio
    (Jupyter, Airflow). En le lançant dans un thread SANS boucle asyncio, on
    contourne le problème tout en gardant l'API synchrone, plus simple.
    """
    result: dict = {}

    def _target():
        try:
            result["value"] = fn(*args, **kwargs)
        except BaseException as exc:  # noqa: BLE001 - remonté au thread appelant
            result["error"] = exc

    t = threading.Thread(target=_target, daemon=True)
    t.start()
    t.join()

    if "error" in result:
        raise result["error"]
    return result.get("value")


# ---------------------------------------------------------------------------
# Session notaire : détient un navigateur Playwright headless persistant
# ---------------------------------------------------------------------------
class _NotaireSession:
    """Détenteur léger d'un navigateur Playwright headless réutilisable.

    Le navigateur (et son contexte, porteur des cookies F5) est lancé
    paresseusement dans un thread dédié : TOUTES les interactions Playwright se
    déroulent dans CE thread, car les objets Playwright sync sont liés à la
    boucle d'événements du thread qui les a créés.
    """

    def __init__(self, headless: bool | None = None):
        self.headless = config.NOTAIRE_HEADLESS if headless is None else headless
        # File d'attente de commandes exécutées dans le thread navigateur.
        self._req = None            # (fn, args, kwargs)
        self._resp = None           # {"value": ...} ou {"error": ...}
        self._req_event = threading.Event()
        self._resp_event = threading.Event()
        self._stop = False
        self._thread = None
        self._lock = threading.Lock()

    # -- boucle du thread navigateur ---------------------------------------
    def _browser_loop(self):
        from playwright.sync_api import sync_playwright

        # Le NOUVEAU moteur headless (--headless=new) passe le challenge F5,
        # contrairement à l'ancien (launch(headless=True)). On lance donc
        # toujours au niveau Playwright en headless=False et on force le
        # nouveau moteur via l'argument quand le mode headless est demandé.
        if self.headless:
            args = [NEW_HEADLESS_ARG, *BASE_ARGS]
        else:
            args = list(BASE_ARGS)

        with sync_playwright() as p:
            browser = p.chromium.launch(headless=False, args=args)
            ctx = browser.new_context(
                locale="fr-BE",
                user_agent=USER_AGENT,
            )
            # Masque navigator.webdriver comme dans le fichier du prof.
            ctx.add_init_script(
                "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"
            )
            # Restaure d'éventuels cookies F5 en cache (best effort).
            self._load_cached_cookies(ctx)
            page = ctx.new_page()

            try:
                while True:
                    self._req_event.wait()
                    self._req_event.clear()
                    if self._stop:
                        break
                    fn, args, kwargs = self._req
                    try:
                        self._resp = {"value": fn(ctx, page, *args, **kwargs)}
                    except BaseException as exc:  # noqa: BLE001
                        self._resp = {"error": exc}
                    self._resp_event.set()
            finally:
                try:
                    self._save_cached_cookies(ctx)
                except Exception:  # noqa: BLE001
                    pass
                try:
                    browser.close()
                except Exception:  # noqa: BLE001
                    pass

    def _ensure_thread(self):
        if self._thread is not None and self._thread.is_alive():
            return
        self._stop = False
        self._req_event.clear()
        self._resp_event.clear()
        self._thread = threading.Thread(target=self._browser_loop, daemon=True)
        self._thread.start()

    def _call(self, fn, *args, **kwargs):
        """Poste ``fn(ctx, page, *args)`` dans le thread navigateur et attend."""
        with self._lock:
            self._ensure_thread()
            self._resp_event.clear()
            self._req = (fn, args, kwargs)
            self._resp = None
            self._req_event.set()
            self._resp_event.wait()
            resp = self._resp
        if "error" in resp:
            raise resp["error"]
        return resp["value"]

    # -- cache cookies ------------------------------------------------------
    def _load_cached_cookies(self, ctx):
        if COOKIE_FILE.exists():
            try:
                cookies = json.loads(COOKIE_FILE.read_text())
                if cookies:
                    ctx.add_cookies(cookies)
                    log.info("Cookies notaire chargés depuis %s", COOKIE_FILE)
            except Exception as exc:  # noqa: BLE001
                log.warning("Cookies notaire illisibles (%s) — ignorés", exc)

    def _save_cached_cookies(self, ctx):
        try:
            COOKIE_FILE.write_text(json.dumps(ctx.cookies(), indent=2))
        except Exception as exc:  # noqa: BLE001
            log.debug("Impossible de sauver les cookies notaire : %s", exc)

    # -- primitive : résout le challenge F5 puis GET JSON -------------------
    @staticmethod
    def _warm_and_get(ctx, page, num10: str, path: str, params: dict):
        """Exécuté DANS le thread navigateur.

        1. goto la page front-end (domcontentloaded) pour amorcer le challenge.
        2. poll l'endpoint API voulu jusqu'à obtenir du JSON.
        Renvoie (status_code, content_type, body_bytes, json_or_None).
        """
        num_short = _num_short(num10)
        front = f"{BASE}/enterprise/{num_short}/statutes"
        try:
            page.goto(front, wait_until="domcontentloaded", timeout=30_000)
        except Exception as exc:  # noqa: BLE001 - le goto peut timeouter, on poll quand même
            log.debug("goto front-end %s : %s", front, exc)

        url = f"{BASE}{path}"
        headers = {
            "Accept": "application/json, text/plain, */*",
            "Referer": front,
        }
        last = (None, "", b"", None)
        for i in range(POLL_ATTEMPTS):
            wait = POLL_INTERVAL_MS
            try:
                resp = ctx.request.get(
                    url, params=params, headers=headers, timeout=20_000
                )
                ctype = (resp.headers or {}).get("content-type", "")
                body = resp.body()
                if "application/json" in ctype:
                    try:
                        data = resp.json()
                    except Exception:  # noqa: BLE001
                        data = None
                    return resp.status, ctype, body, data
                last = (resp.status, ctype, body, None)
                # 429 = rate-limit du portail : on temporise plus longuement.
                if resp.status == 429:
                    wait = RATE_LIMIT_WAIT_MS
            except Exception as exc:  # noqa: BLE001
                log.debug("poll %s tentative %d : %s", url, i, exc)
            page.wait_for_timeout(wait)
        if last[0] == 429:
            log.warning("Rate-limit portail (429) non levé pour %s", url)
        else:
            log.warning(
                "Challenge F5 non résolu pour %s (dernier ctype=%r status=%s)",
                url, last[1], last[0],
            )
        return last

    @staticmethod
    def _get_binary(ctx, page, num10: str, path: str):
        """GET binaire (PDF) via le ctx. -> (status, ctype, bytes).

        L'endpoint PDF non-certifié re-challenge parfois (F5) : comme
        ``ctx.request`` n'exécute pas le JS, on re-``goto`` la page front-end
        (qui, elle, résout le défi) avant chaque tentative, puis on réessaie
        jusqu'à obtenir un vrai PDF.
        """
        num_short = _num_short(num10)
        front = f"{BASE}/enterprise/{num_short}/statutes"
        url = f"{BASE}{path}"
        headers = {"Accept": "application/pdf,*/*", "Referer": front}
        last = (None, "", b"")
        for i in range(PDF_ATTEMPTS):
            try:
                page.goto(front, wait_until="domcontentloaded", timeout=30_000)
                page.wait_for_timeout(1000)
            except Exception as exc:  # noqa: BLE001 - on tente le GET quand même
                log.debug("goto avant PDF (tentative %d) : %s", i, exc)
            wait = POLL_INTERVAL_MS
            try:
                resp = ctx.request.get(url, headers=headers, timeout=30_000)
                ctype = (resp.headers or {}).get("content-type", "")
                body = resp.body()
                if resp.status == 404:
                    return resp.status, ctype, body
                if resp.status == 200 and (body[:4] == b"%PDF" or "pdf" in ctype):
                    return resp.status, ctype, body
                last = (resp.status, ctype, body)
                if resp.status == 429:
                    wait = RATE_LIMIT_WAIT_MS
            except Exception as exc:  # noqa: BLE001
                log.debug("GET PDF tentative %d : %s", i, exc)
            page.wait_for_timeout(wait)
        return last

    # -- API interne utilisée par les fonctions module ---------------------
    def api_json(self, num10: str, path: str, params: dict):
        return self._call(self._warm_and_get, num10, path, params)

    def api_binary(self, num10: str, path: str):
        return self._call(self._get_binary, num10, path)

    def close(self):
        with self._lock:
            if self._thread is None or not self._thread.is_alive():
                return
            self._stop = True
            self._resp_event.clear()
            self._req_event.set()
        self._thread.join(timeout=15)


# ---------------------------------------------------------------------------
# Session module-level paresseuse (réutilisée entre appels par efficacité)
# ---------------------------------------------------------------------------
_SESSION: _NotaireSession | None = None
_SESSION_LOCK = threading.Lock()

# Session DÉDIÉE au téléchargement des PDF. IMPORTANT : l'endpoint PDF
# non-certifié n'accepte le token F5 QUE dans un contexte « frais » dont la
# première action est justement le PDF ; un contexte qui a d'abord interrogé
# l'API JSON des statuts se fait ensuite refuser le PDF (re-challenge en boucle).
# On isole donc les PDF dans leur propre navigateur/contexte.
_PDF_SESSION: _NotaireSession | None = None
_PDF_SESSION_LOCK = threading.Lock()


def get_session(headless: bool | None = None) -> _NotaireSession:
    """Renvoie la session notaire partagée pour l'API JSON (statuts).

    Réutilisée entre appels : le challenge F5 n'est résolu qu'une fois, puis
    les cookies portés par le contexte accélèrent les requêtes suivantes.
    """
    global _SESSION
    with _SESSION_LOCK:
        if _SESSION is None:
            _SESSION = _NotaireSession(headless=headless)
        return _SESSION


def get_pdf_session(headless: bool | None = None) -> _NotaireSession:
    """Session dédiée au téléchargement PDF (contexte séparé de l'API JSON)."""
    global _PDF_SESSION
    with _PDF_SESSION_LOCK:
        if _PDF_SESSION is None:
            _PDF_SESSION = _NotaireSession(headless=headless)
        return _PDF_SESSION


def reset_session():
    """Ferme les sessions partagées (statuts + PDF) et libère les navigateurs."""
    global _SESSION, _PDF_SESSION
    with _SESSION_LOCK:
        if _SESSION is not None:
            _SESSION.close()
            _SESSION = None
    with _PDF_SESSION_LOCK:
        if _PDF_SESSION is not None:
            _PDF_SESSION.close()
            _PDF_SESSION = None


# ---------------------------------------------------------------------------
# API publique
# ---------------------------------------------------------------------------
def get_statutes(bce_num10: str, session: _NotaireSession | None = None) -> list[dict]:
    """Renvoie les statuts au statut ``DONE`` pour une entreprise.

    ``bce_num10`` : numéro BCE ; normalisé en 10 chiffres avec zéro de tête.
    Utilise la méthode navigateur-headless + polling pour franchir le mur F5,
    puis pagine l'API JSON.
    """
    num10 = _num10(bce_num10)
    sess = session or get_session()

    all_statutes: list[dict] = []
    offset = 0
    while True:
        status, ctype, _body, data = sess.api_json(
            num10,
            f"/api/enterprises/{num10}/statutes",
            {"deedDate": "", "offset": offset, "limit": PAGE_SIZE},
        )
        if data is None:
            log.error(
                "[%s] réponse non-JSON (status=%s ctype=%r) — abandon",
                num10, status, ctype,
            )
            break

        batch = data.get("statutes", []) or []
        total = data.get("totalItems", 0) or 0
        all_statutes.extend(batch)
        log.info(
            "[%s] offset=%d — %d statuts (total: %d)",
            num10, offset, len(batch), total,
        )

        if not batch or len(all_statutes) >= total:
            break
        offset += PAGE_SIZE
        time.sleep(config.NOTAIRE_DELAY)

    done = [s for s in all_statutes if s.get("documentStatus") == "DONE"]
    log.info("[%s] -> %d DONE", num10, len(done))
    return done


def fetch_statute_pdf_bytes(
    bce_num10: str, statute: dict, session: _NotaireSession | None = None
) -> bytes | None:
    """Télécharge le PDF non-certifié d'un statut. -> bytes ou None.

    Passe par le contexte Playwright (cookies F5 portés). Renvoie None si
    l'endpoint répond 404 ou ne renvoie pas un vrai PDF.
    """
    num10 = _num10(bce_num10)
    doc_id = statute.get("documentId")
    if not doc_id:
        log.warning("[%s] statut sans documentId — ignoré", num10)
        return None

    sess = session or get_pdf_session()   # contexte dédié PDF (voir get_pdf_session)
    path = f"/api/enterprises/{num10}/statutes/non-certified/{doc_id}"
    try:
        status, ctype, body = sess.api_binary(num10, path)
    except Exception as exc:  # noqa: BLE001
        log.error("[%s] échec PDF %s : %s", num10, doc_id, exc)
        return None

    if status == 404:
        log.info("[%s] PDF %s : 404", num10, doc_id)
        return None
    if status >= 400:
        log.warning("[%s] PDF %s : HTTP %s", num10, doc_id, status)
        return None
    # Validation : vrai PDF (magic bytes) ou content-type pdf + taille plausible.
    if body[:4] == b"%PDF":
        log.info("[%s] PDF %s : %d KB", num10, doc_id, len(body) // 1024)
        return body
    if "pdf" in ctype and len(body) >= 1000:
        return body
    log.warning(
        "[%s] PDF %s : contenu inattendu (ctype=%r, %d octets)",
        num10, doc_id, ctype, len(body),
    )
    return None


def needs_notaire_check(forme_juridique: str, status: str = "Active") -> bool:
    """True si une entreprise active peut avoir un statut chez le notaire.

    Reprend la logique du prof : on saute les formes juridiques connues pour ne
    jamais avoir de statut notarié (personnes physiques, entités publiques…).
    """
    return status == "Active" and forme_juridique not in NO_NOTAIRE_FORMS


# ---------------------------------------------------------------------------
# Démo (garde __main__ — ne tourne pas à l'import)
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(levelname)-8s %(message)s",
        datefmt="%H:%M:%S",
    )

    ENTREPRISES = {
        "Google Belgium": "0878065378",
        "SNCB":           "0203430576",
    }

    for nom, bce in ENTREPRISES.items():
        log.info("%s\n%s (%s)", "=" * 50, nom, bce)
        statutes = get_statutes(bce)
        if not statutes:
            log.info("  Aucun statut DONE.")
            continue
        for s in statutes:
            log.info(
                "  %s  %s  (docId=%s)",
                s.get("deedDate"), s.get("documentTitle"), s.get("documentId"),
            )
            pdf = fetch_statute_pdf_bytes(bce, s)
            log.info("    PDF -> %s", f"{len(pdf)} octets" if pdf else "None")

    reset_session()
