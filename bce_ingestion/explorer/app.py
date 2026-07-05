"""Explorateur d'entreprises BCE — application Flask.

Lancement :
    python -m bce_ingestion.explorer.app          # http://127.0.0.1:5001
    BCE_EXPLORER_PORT=8080 python -m bce_ingestion.explorer.app

Endpoints JSON (données 100 % réelles issues de MongoDB) :
    GET /api/stats
    GET /api/search?q=<terme>&sector=<all|hotel>&limit=<n>&offset=<n>
    GET /api/company/<bce>
    GET /api/dashboard                              (agrégats du dashboard)
    GET /api/document?bce=<b>&source=<s>&ref=<r>   (fichier Bronze réel ou aperçu)
"""
from __future__ import annotations

import os

from flask import Flask, abort, jsonify, render_template, request, send_file

from . import data_source

app = Flask(__name__)

# Racine locale de la couche Bronze pour servir les fichiers réels (si présents).
BRONZE_LOCAL = os.environ.get("BCE_BRONZE_LOCAL", "/data/bronze")

# Dashboard agrégé (page HTML autonome à la racine du repo).
DASHBOARD_HTML = os.environ.get(
    "BCE_DASHBOARD_HTML",
    os.path.join(os.path.dirname(__file__), "..", "..", "bce-dashboard.html"))


def _source():
    """Résout la source à chaque requête (permet de brancher Mongo à chaud).

    Ne met en cache QUE si la connexion réussit : si Mongo est down,
    ``get_source()`` lève ``SourceUnavailable`` et l'on retentera au prochain
    appel (branchement de Mongo « à chaud » sans redémarrer l'app).
    """
    src = getattr(app, "_source", None)
    if src is None:
        src = app._source = data_source.get_source()
    return src


@app.errorhandler(data_source.SourceUnavailable)
def _handle_source_unavailable(exc):
    """MongoDB injoignable -> réponse explicite (jamais de données inventées)."""
    message = str(exc)
    if request.path.startswith("/api/"):
        return jsonify({"error": "source_unavailable", "message": message}), 503
    html = (
        "<!doctype html><meta charset='utf-8'>"
        "<title>Source indisponible</title>"
        "<div style=\"font-family:system-ui,sans-serif;max-width:520px;margin:18vh auto;"
        "padding:26px 30px;border:1px solid #E3E7ED;border-radius:12px;color:#3A414D\">"
        "<h2 style='margin:0 0 8px'>MongoDB injoignable</h2>"
        "<p style='color:#697384;line-height:1.5'>L'explorateur ne sert que des "
        "données réelles : aucune donnée synthétique de repli. Démarrez MongoDB "
        "puis peuplez la base.</p>"
        f"<pre style='background:#F7F8FA;border:1px solid #EDF0F4;border-radius:6px;"
        f"padding:10px;white-space:pre-wrap;font-size:.8rem'>{message}</pre></div>"
    )
    return html, 503


def _company_payload(bce: str) -> dict | None:
    company = _source().get_company(bce)
    if not company:
        return None
    # Série financière (dépôts NBB). Une entreprise peut déposer PLUSIEURS
    # comptes pour un même exercice (consolidé + statutaire, schémas distincts) :
    # on ne garde qu'UN dépôt par année — le plus complet (le plus de KPIs
    # renseignés) — pour une visualisation à une colonne par exercice.
    by_year: dict = {}
    for d in company.get("documents", []):
        if d.get("source") != "nbb" or not d.get("financials"):
            continue
        f = d["financials"]
        year = f.get("year")
        filled = sum(1 for v in f.values() if v is not None)
        if year not in by_year or filled > by_year[year][0]:
            by_year[year] = (filled, f)
    fin = [v[1] for v in by_year.values()]
    fin.sort(key=lambda f: f["year"])
    company = dict(company)
    company["financials"] = fin
    return company


def _asset_version() -> str:
    """Empreinte des assets statiques (max mtime) -> cache-busting des URL.

    Change dès qu'app.js/styles.css est modifié : le navigateur refetch au lieu
    de resservir une version en cache (sinon la pagination JS reste invisible).
    """
    static_dir = os.path.join(os.path.dirname(__file__), "static")
    latest = 0.0
    try:
        for name in os.listdir(static_dir):
            latest = max(latest, os.path.getmtime(os.path.join(static_dir, name)))
    except OSError:
        pass
    return str(int(latest))


@app.route("/")
def index():
    src = _source()
    return render_template("index.html", mode=src.mode, stats=src.stats(),
                           asset_v=_asset_version())


@app.route("/dashboard")
def dashboard():
    path = os.path.realpath(DASHBOARD_HTML)
    if not os.path.isfile(path):
        abort(404, description="Dashboard introuvable (bce-dashboard.html)")
    return send_file(path)


@app.route("/api/stats")
def api_stats():
    src = _source()
    return jsonify({"mode": src.mode, **src.stats()})


@app.route("/api/search")
def api_search():
    q = request.args.get("q", "")
    sector = request.args.get("sector", "all")
    try:
        limit = min(max(int(request.args.get("limit", 40)), 1), 200)
    except (TypeError, ValueError):
        limit = 40
    try:
        offset = max(int(request.args.get("offset", 0)), 0)
    except (TypeError, ValueError):
        offset = 0
    with_docs = request.args.get("docs", "") in ("1", "true", "yes")
    page = _source().search(q, sector=sector, limit=limit, offset=offset,
                            with_docs=with_docs)
    results = page["results"]
    return jsonify({"mode": _source().mode, "total": page["total"],
                    "offset": offset, "limit": limit,
                    "count": len(results), "results": results})


@app.route("/api/dashboard")
def api_dashboard():
    """Agrégats réels du dashboard (calculés en direct depuis MongoDB)."""
    from . import dashboard
    return jsonify(dashboard.payload(_source()._db))


@app.route("/api/company/<bce>")
def api_company(bce: str):
    company = _company_payload(bce)
    if not company:
        abort(404, description="Entreprise introuvable")
    return jsonify(company)


def _safe_local_path(hdfs_path: str) -> str | None:
    """Traduit un chemin HDFS Bronze en chemin local sûr sous BRONZE_LOCAL.

    Renvoie None si le fichier n'existe pas ou sort de la racine (anti-traversée).
    """
    if not hdfs_path or hdfs_path == "—":
        return None
    root = os.path.realpath(BRONZE_LOCAL)

    # Cas backend "local" : hdfs_path est déjà un chemin local absolu
    # (ex. /.../bronze_local/<bce>/nbb/<year>/<ref>.csv). On l'utilise tel quel
    # s'il vit bien sous la racine Bronze.
    abs_candidate = os.path.realpath(hdfs_path)
    if os.path.commonpath([root, abs_candidate]) == root and os.path.isfile(abs_candidate):
        return abs_candidate

    # Cas backend HDFS : hdfs:///bronze/<bce>/... -> <bce>/... sous BRONZE_LOCAL.
    rel = hdfs_path.split("://", 1)[-1].lstrip("/")
    for prefix in ("bronze/", ""):
        if rel.startswith(prefix):
            rel = rel[len(prefix):]
            break
    candidate = os.path.realpath(os.path.join(root, rel))
    if os.path.commonpath([root, candidate]) != root:
        return None  # tentative de traversée de chemin
    return candidate if os.path.isfile(candidate) else None


@app.route("/api/document")
def api_document():
    bce = data_source.normalize_bce(request.args.get("bce", ""))
    source = request.args.get("source", "")
    ref = request.args.get("ref", "")

    company = _source().get_company(bce)
    if not company:
        abort(404)
    # Le document DOIT figurer dans les métadonnées de l'entreprise
    # (on ne sert jamais un chemin fourni librement par le client).
    doc = next((d for d in company.get("documents", [])
                if d.get("source") == source and str(d.get("ref")) == str(ref)), None)
    if not doc:
        abort(404, description="Document introuvable")

    local = _safe_local_path(doc.get("hdfs_path", ""))
    if local:
        return _serve_bronze_file(local)

    # Mode démo / fichier absent : aperçu explicatif (pas d'erreur navigateur).
    return _preview_placeholder(company, doc)


# Types servables *en ligne* (aucun n'exécute de script). Tout le reste est
# forcé en téléchargement pour éviter qu'un HTML/SVG ingéré ne s'exécute.
_INLINE_MIME = {".pdf": "application/pdf", ".csv": "text/csv",
                ".txt": "text/plain", ".json": "application/json"}


def _serve_bronze_file(local: str):
    ext = os.path.splitext(local)[1].lower()
    mime = _INLINE_MIME.get(ext)
    resp = send_file(
        local,
        mimetype=mime or "application/octet-stream",
        as_attachment=mime is None,   # type inconnu -> téléchargement, pas d'affichage
        download_name=os.path.basename(local),
    )
    # Défense en profondeur : pas de sniffing, et sandbox stricte via CSP au cas
    # où un contenu HTML passerait malgré tout (l'iframe est aussi sandboxée).
    resp.headers["X-Content-Type-Options"] = "nosniff"
    resp.headers["Content-Security-Policy"] = "sandbox; default-src 'none'"
    return resp


def _preview_placeholder(company: dict, doc: dict):
    esc = lambda s: (str(s).replace("&", "&amp;").replace("<", "&lt;")
                     .replace(">", "&gt;"))
    html = f"""<!doctype html><html lang="fr"><head><meta charset="utf-8">
<style>
body{{margin:0;font-family:ui-sans-serif,system-ui,sans-serif;color:#3A414D;
background:#F1F3F6;display:grid;place-items:center;height:100vh}}
.card{{background:#fff;border:1px solid #E3E7ED;border-radius:12px;padding:28px 30px;
max-width:440px;box-shadow:0 3px 12px rgba(26,29,36,.06);text-align:center}}
.ic{{width:48px;height:48px;border-radius:12px;margin:0 auto 14px;display:grid;
place-items:center;background:#F4EAD4;color:#8A621B;font-size:22px}}
h2{{margin:0 0 6px;font-size:1.05rem}}
p{{margin:6px 0;font-size:.85rem;color:#697384;line-height:1.5}}
code{{font-family:ui-monospace,Menlo,Consolas,monospace;font-size:.76rem;
background:#F7F8FA;border:1px solid #EDF0F4;border-radius:5px;padding:2px 6px;
color:#3A414D;word-break:break-all}}
.tag{{display:inline-block;font-family:ui-monospace,monospace;font-size:.7rem;
padding:3px 9px;border-radius:999px;background:#F4EAD4;color:#8A621B;margin-top:8px}}
</style></head><body><div class="card">
<div class="ic">📄</div>
<h2>{esc(doc.get('title') or 'Document Bronze')}</h2>
<p><b>{esc(company['denomination'])}</b> · {esc(company['bce_formatted'])}</p>
<p>Aperçu du binaire non disponible en <b>mode démo</b> : le fichier vit sur HDFS
et n'est pas téléchargé dans cet environnement.</p>
<p>Chemin Bronze cible :<br><code>{esc(doc.get('hdfs_path'))}</code></p>
<span class="tag">{esc(doc.get('source'))} · {esc(doc.get('kind'))} · {esc(doc.get('status'))}</span>
</div></body></html>"""
    return html, 200, {"Content-Type": "text/html; charset=utf-8"}


def main() -> None:
    port = int(os.environ.get("BCE_EXPLORER_PORT", "5001"))
    src = _source()
    print(f"[explorer] source = {src.mode}  |  stats = {src.stats()}")
    print(f"[explorer] http://127.0.0.1:{port}")
    app.run(host="127.0.0.1", port=port, debug=False)


if __name__ == "__main__":
    main()
