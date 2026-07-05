"use strict";
(function () {
  var nf = new Intl.NumberFormat("fr-BE");
  var eur = new Intl.NumberFormat("fr-BE", { style: "currency", currency: "EUR", maximumFractionDigits: 0 });
  var SRC_LABELS = { nbb: "NBB / CBSO — comptes annuels", ejustice: "eJustice — Moniteur belge", notaire: "Notaire.be — statuts" };

  function el(id) { return document.getElementById(id); }
  var ESC_MAP = { "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" };
  // Échappe &<>"' -> sûr en contenu texte ET en valeur d'attribut à guillemets.
  function esc(s) { return (s == null ? "" : String(s)).replace(/[&<>"']/g, function (c) { return ESC_MAP[c]; }); }
  // Liste blanche des classes de statut (jamais de valeur brute dans class="").
  var STATUS_CLASSES = { ac: 1, ju: 1, st: 1, done: 1, in_progress: 1, progress: 1, pending: 1, error: 1 };
  function statusCls(s) { var v = String(s == null ? "" : s).toLowerCase(); return STATUS_CLASSES[v] ? v : ""; }
  function api(path) { return fetch(path).then(function (r) { if (!r.ok) throw new Error(r.status); return r.json(); }); }

  var state = { q: "", sector: "all", active: null, seq: 0, lastFocus: null };

  /* ---------- header stats ---------- */
  api("/api/stats").then(function (s) {
    var demo = s.mode === "demo";
    el("mode-dot").className = "dot " + s.mode;
    el("mode-txt").textContent = demo ? "Mode démo (données représentatives)" : "MongoDB — données réelles";
    el("stat-companies").textContent = nf.format(s.companies) + " entreprises";
    el("stat-docs").textContent = nf.format(s.documents) + " documents";
  }).catch(function () { el("mode-txt").textContent = "source indisponible"; });

  /* ---------- search ---------- */
  var tmr = null;
  el("q").addEventListener("input", function (e) {
    state.q = e.target.value;
    clearTimeout(tmr);
    tmr = setTimeout(runSearch, 180);
  });
  Array.prototype.forEach.call(document.querySelectorAll(".seg-btn"), function (b) {
    b.addEventListener("click", function () {
      document.querySelectorAll(".seg-btn").forEach(function (x) {
        x.classList.remove("is-on"); x.setAttribute("aria-selected", "false");
      });
      b.classList.add("is-on"); b.setAttribute("aria-selected", "true");
      state.sector = b.dataset.sector;
      runSearch();
    });
  });

  function runSearch() {
    var seq = ++state.seq;  // jeton de séquence -> ignore les réponses périmées
    var url = "/api/search?sector=" + encodeURIComponent(state.sector) +
      "&q=" + encodeURIComponent(state.q);
    api(url).then(function (data) {
      if (seq !== state.seq) return;  // une recherche plus récente a pris la main
      renderResults(data);
    }).catch(function () {
      if (seq !== state.seq) return;
      el("results-list").innerHTML = '<div class="loading">Erreur de recherche.</div>';
    });
  }

  function srcBadges(dc) {
    return ["nbb", "notaire", "ejustice"].filter(function (s) { return dc[s]; })
      .map(function (s) {
        return '<span class="badge"><span class="bd ' + s + '"></span><b>' + dc[s] + '</b> ' + s + '</span>';
      }).join("");
  }

  function renderResults(data) {
    el("results-count").textContent = data.count + (data.count === 1 ? " résultat" : " résultats");
    var host = el("results-list");
    if (!data.count) {
      host.innerHTML = '<div class="loading">Aucune entreprise ne correspond.</div>';
      return;
    }
    host.innerHTML = data.results.map(function (c) {
      var hotel = c.is_hospitality ? '<span class="pill hotel">hôtellerie</span>' : "";
      return '<button class="rcard" data-bce="' + esc(c.bce) + '">' +
        '<div class="rc-name">' + esc(c.denomination) + '</div>' +
        '<div class="rc-meta"><span class="rc-bce">' + esc(c.bce_formatted) + '</span>' +
        '<span class="pill ' + statusCls(c.status) + '">' + esc(c.status_label) + '</span>' + hotel + '</div>' +
        '<div class="rc-meta"><span class="rc-bce">' + esc(c.nace_code) + ' · ' + esc(c.nace_label) + '</span></div>' +
        (c.doc_counts.total ? '<div class="rc-badges">' + srcBadges(c.doc_counts) + '</div>' : "") +
        '</button>';
    }).join("");
    Array.prototype.forEach.call(host.querySelectorAll(".rcard"), function (card) {
      card.addEventListener("click", function () { openCompany(card.dataset.bce, card); });
    });
    // ré-applique l'état actif si présent
    if (state.active) {
      var a = host.querySelector('.rcard[data-bce="' + state.active + '"]');
      if (a) a.classList.add("is-active");
    }
  }

  /* ---------- company detail ---------- */
  function openCompany(bce, card) {
    state.active = bce;
    document.querySelectorAll(".rcard").forEach(function (c) { c.classList.remove("is-active"); });
    if (card) card.classList.add("is-active");
    el("detail").innerHTML = '<div class="panel"><div class="loading"><div class="spin"></div>Chargement…</div></div>';
    api("/api/company/" + encodeURIComponent(bce)).then(renderCompany).catch(function () {
      // fiche introuvable : on lève la sélection fantôme
      state.active = null;
      if (card) card.classList.remove("is-active");
      el("detail").innerHTML = '<div class="panel"><div class="loading">Entreprise introuvable.</div></div>';
    });
  }

  function fact(k, v, mono) {
    return '<div class="fact"><div class="fk">' + k + '</div><div class="fv' + (mono ? " mono" : "") + '">' + v + '</div></div>';
  }

  function renderCompany(c) {
    var hotel = c.is_hospitality ? '<span class="pill hotel">hôtellerie ^55</span>' : "";
    var head =
      '<div class="panel"><div class="co-head"><div>' +
      '<h2>' + esc(c.denomination) + '</h2>' +
      '<div class="co-bce">n° BCE ' + esc(c.bce_formatted) + '</div>' +
      '<div class="co-pills"><span class="pill ' + statusCls(c.status) + '">' + esc(c.status_label) + '</span>' + hotel + '</div>' +
      '</div></div>' +
      '<div class="co-facts">' +
      fact("Forme juridique", esc(c.form_label || "—")) +
      fact("Activité (NACE)", esc(c.nace_code) + " · " + esc(c.nace_label), true) +
      fact("Région", esc(c.region || "—") + (c.city ? " · " + esc(c.city) : "")) +
      fact("Début d'activité", esc(c.start_date || "—"), true) +
      fact("Documents liés", esc(c.doc_counts.total)) +
      '</div></div>';

    el("detail").innerHTML = head + financialsHTML(c) + documentsHTML(c);
    bindDocButtons();
    animateFin();
  }

  /* ---------- financials (panneau exhaustif) ---------- */
  function money(v) { return v == null ? "—" : eur.format(v); }
  function pctv(v) { return v == null ? "—" : (String(v).replace(".", ",") + " %"); }
  function xv(v) { return v == null ? "—" : (String(v).replace(".", ",") + "×"); }
  function daysv(v) { return v == null ? "—" : (nf.format(v) + " j"); }
  function growthCls(v) { return v == null ? "" : (v >= 0 ? "pos" : "neg"); }
  function growthTxt(v) { return v == null ? "—" : ((v > 0 ? "+" : "") + String(v).replace(".", ",") + " %"); }

  function ratioGroup(title, items) {
    var cells = items.map(function (r) {
      return '<div class="ratio"><div class="rk">' + esc(r[0]) + '</div>' +
        '<div class="rv ' + (r[3] || "") + '">' + r[1] + '</div><div class="rh">' + esc(r[2]) + '</div></div>';
    }).join("");
    return '<div class="sub-title">' + esc(title) + '</div><div class="ratios">' + cells + '</div>';
  }

  function financialsHTML(c) {
    var fin = c.financials || [];
    if (!fin.length) return "";
    var last = fin[fin.length - 1];
    var prev = fin.length > 1 ? fin[fin.length - 2] : null;
    function yoy(key) { return prev && prev[key] ? Math.round((last[key] / prev[key] - 1) * 1000) / 10 : null; }

    // -- tuiles monétaires (dernier exercice) --
    var tiles = [
      ["Chiffre d'affaires", money(last.ca), "code 70", "var(--gold)"],
      ["Marge brute", money(last.marge_brute), last.taux_marge_brute + " % du CA", "var(--gold-deep)"],
      ["Valeur ajoutée", money(last.valeur_ajoutee), last.taux_va + " % du CA", "#B07D2B"],
      ["EBITDA", money(last.ebitda), last.taux_ebitda + " % du CA", "var(--nbb)"],
      ["EBIT", money(last.ebit), "code 9901", "#4E79A7"],
      ["Résultat net", money(last.resultat_net), last.marge_nette + " % · code 9904", "var(--done)"],
      ["Capacité d'autofin.", money(last.caf), "rés. net + amort.", "#5FBF92"],
      ["Fonds propres", money(last.fonds_propres), "code 10/15", "var(--gold)"],
      ["Total de l'actif", money(last.total_actif), "code 20/58", "#8B94A3"],
      ["Trésorerie", money(last.tresorerie), "code 54/58", "var(--progress)"]
    ];
    var tilesHTML = tiles.map(function (k) {
      return '<div class="kpi" style="--accent:' + k[3] + '"><div class="kk">' + esc(k[0]) + '</div>' +
        '<div class="kv">' + k[1] + '</div><div class="ks">' + esc(k[2]) + '</div></div>';
    }).join("");

    // -- groupes de ratios --
    var groups =
      ratioGroup("Croissance (annuelle)", [
        ["Chiffre d'affaires", growthTxt(yoy("ca")), prev ? "vs " + prev.year : "1er exercice", growthCls(yoy("ca"))],
        ["EBITDA", growthTxt(yoy("ebitda")), prev ? "vs " + prev.year : "—", growthCls(yoy("ebitda"))],
        ["Résultat net", growthTxt(yoy("resultat_net")), prev ? "vs " + prev.year : "—", growthCls(yoy("resultat_net"))],
        ["Valeur ajoutée", growthTxt(yoy("valeur_ajoutee")), prev ? "vs " + prev.year : "—", growthCls(yoy("valeur_ajoutee"))],
        ["Effectif", growthTxt(yoy("effectif")), prev ? "vs " + prev.year : "—", growthCls(yoy("effectif"))]
      ]) +
      ratioGroup("Rentabilité (marges)", [
        ["Taux de marge brute", pctv(last.taux_marge_brute), "marge brute / CA"],
        ["Taux de valeur ajoutée", pctv(last.taux_va), "VA / CA"],
        ["Marge d'EBITDA", pctv(last.taux_ebitda), "EBITDA / CA"],
        ["Marge d'exploitation", pctv(last.marge_ebit), "EBIT / CA"],
        ["Marge avant impôt", pctv(last.marge_avant_impot), "9903 / CA"],
        ["Marge nette", pctv(last.marge_nette), "rés. net / CA"]
      ]) +
      ratioGroup("Rendement des capitaux", [
        ["ROE", pctv(last.roe), "rés. net / fonds propres"],
        ["ROA", pctv(last.roa), "rés. net / total actif"],
        ["ROCE", pctv(last.roce), "EBIT / capitaux engagés"],
        ["Rotation de l'actif", xv(last.rotation_actif), "CA / total actif"]
      ]) +
      ratioGroup("Structure & solvabilité", [
        ["Autonomie financière", pctv(last.autonomie_fin), "fonds propres / actif"],
        ["Taux d'endettement", pctv(last.taux_endettement), "dettes / actif"],
        ["Gearing", pctv(last.gearing), "dettes fin. / fonds propres"],
        ["Dette nette / EBITDA", xv(last.dette_ebitda), "levier d'endettement"],
        ["Couverture des intérêts", xv(last.couverture_interets), "EBIT / charges fin."],
        ["Capacité de rembours.", xv(last.capacite_remboursement), "dettes fin. / CAF"]
      ]) +
      ratioGroup("Liquidité", [
        ["Liquidité générale", xv(last.current_ratio), "actif circ. / dettes CT"],
        ["Liquidité réduite", xv(last.quick_ratio), "(circ.−stocks) / dettes CT"],
        ["Ratio de trésorerie", xv(last.cash_ratio), "tréso / dettes CT"],
        ["Poids de la trésorerie", pctv(last.poids_treso), "tréso / actif"]
      ]) +
      ratioGroup("Rotation & délais", [
        ["Délai clients (DSO)", daysv(last.dso), "365 × créances / CA"],
        ["Délai fournisseurs (DPO)", daysv(last.dpo), "365 × dettes comm. / achats"],
        ["Stocks (DIO)", daysv(last.dio), "365 × stocks / achats"],
        ["Rotation des stocks", xv(last.rotation_stocks), "achats / stocks"]
      ]) +
      ratioGroup("Productivité & personnel", [
        ["CA par ETP", money(last.ca_par_etp), "~" + last.effectif + " ETP estimés"],
        ["Valeur ajoutée / ETP", money(last.va_par_etp), "productivité"],
        ["Résultat net / ETP", money(last.resultat_par_etp), "par salarié"],
        ["Charges de personnel / CA", pctv(last.charges_perso_sur_ca), "code 62 / CA"],
        ["Partage de la VA", pctv(last.partage_va), "charges pers. / VA"],
        ["Taux d'imposition eff.", pctv(last.taux_imposition), "impôts / rés. avant impôt"]
      ]);

    // -- table pluriannuelle (tous les postes) --
    var rows = [
      { group: "Compte de résultat" },
      { label: "Chiffre d'affaires", key: "ca", fmt: "money", code: "70" },
      { label: "Coût des ventes", key: "cogs", fmt: "money", code: "60" },
      { label: "Marge brute", key: "marge_brute", fmt: "money" },
      { label: "Valeur ajoutée", key: "valeur_ajoutee", fmt: "money", code: "9800" },
      { label: "Charges de personnel", key: "charges_personnel", fmt: "money", code: "62" },
      { label: "EBITDA", key: "ebitda", fmt: "money" },
      { label: "Amortissements", key: "depreciation", fmt: "money", code: "630" },
      { label: "EBIT (exploitation)", key: "ebit", fmt: "money", code: "9901" },
      { label: "Charges financières", key: "charges_financieres", fmt: "money", code: "65" },
      { label: "Résultat avant impôt", key: "resultat_avant_impot", fmt: "money", code: "9903" },
      { label: "Impôts", key: "impots", fmt: "money", code: "67/77" },
      { label: "Résultat net", key: "resultat_net", fmt: "money", code: "9904" },
      { label: "Capacité d'autofinancement", key: "caf", fmt: "money" },
      { group: "Bilan — actif" },
      { label: "Actifs immobilisés", key: "immobilisations", fmt: "money", code: "20/28" },
      { label: "Stocks", key: "stocks", fmt: "money", code: "3" },
      { label: "Créances", key: "creances", fmt: "money", code: "40/41" },
      { label: "Trésorerie", key: "tresorerie", fmt: "money", code: "54/58" },
      { label: "Actifs circulants", key: "actifs_circulants", fmt: "money", code: "29/58" },
      { label: "Total de l'actif", key: "total_actif", fmt: "money", code: "20/58" },
      { group: "Bilan — passif" },
      { label: "Fonds propres", key: "fonds_propres", fmt: "money", code: "10/15" },
      { label: "Provisions", key: "provisions", fmt: "money", code: "16" },
      { label: "Dettes fin. > 1 an", key: "dettes_lt", fmt: "money", code: "17" },
      { label: "Dettes fin. ≤ 1 an", key: "dettes_ct_fin", fmt: "money", code: "43" },
      { label: "Dettes commerciales", key: "dettes_comm", fmt: "money", code: "44" },
      { label: "Dettes à court terme", key: "dettes_ct", fmt: "money", code: "42/48" },
      { label: "Total des dettes", key: "dettes_totales", fmt: "money" },
      { label: "Capitaux permanents", key: "capitaux_permanents", fmt: "money" },
      { group: "Équilibres financiers" },
      { label: "Fonds de roulement", key: "fonds_roulement", fmt: "money" },
      { label: "Besoin en FR (BFR)", key: "bfr", fmt: "money" },
      { label: "Trésorerie nette", key: "tresorerie_nette", fmt: "money" },
      { label: "Dette nette", key: "dette_nette", fmt: "money" },
      { label: "Effectif (ETP)", key: "effectif", fmt: "num" },
      { group: "Ratios clés" },
      { label: "Marge d'EBITDA", key: "taux_ebitda", fmt: "pct" },
      { label: "Marge nette", key: "marge_nette", fmt: "pct" },
      { label: "ROE", key: "roe", fmt: "pct" },
      { label: "ROCE", key: "roce", fmt: "pct" },
      { label: "Autonomie financière", key: "autonomie_fin", fmt: "pct" },
      { label: "Liquidité générale", key: "current_ratio", fmt: "x" },
      { group: "Croissance (YoY)" },
      { label: "Croissance du CA", key: "ca", fmt: "growth" },
      { label: "Croissance de l'EBITDA", key: "ebitda", fmt: "growth" },
      { label: "Croissance du résultat net", key: "resultat_net", fmt: "growth" }
    ];
    function fmtCell(row, f, i) {
      if (row.fmt === "money") return money(f[row.key]);
      if (row.fmt === "pct") return pctv(f[row.key]);
      if (row.fmt === "x") return xv(f[row.key]);
      if (row.fmt === "num") return nf.format(f[row.key]);
      if (row.fmt === "growth") {
        if (i === 0) return '<span class="u">—</span>';
        var p = fin[i - 1];
        var g = p && p[row.key] ? Math.round((f[row.key] / p[row.key] - 1) * 1000) / 10 : null;
        return '<span class="' + growthCls(g) + '">' + growthTxt(g) + "</span>";
      }
      return "—";
    }
    var thead = "<tr><th>Poste</th>" + fin.map(function (f) { return "<th>" + f.year + "</th>"; }).join("") + "</tr>";
    var tbody = rows.map(function (row) {
      if (row.group) {
        return '<tr class="group"><td colspan="' + (fin.length + 1) + '">' + esc(row.group) + "</td></tr>";
      }
      var code = row.code ? ' <span class="u">(' + esc(row.code) + ")</span>" : "";
      return "<tr><td>" + esc(row.label) + code + "</td>" +
        fin.map(function (f, i) { return "<td>" + fmtCell(row, f, i) + "</td>"; }).join("") + "</tr>";
    }).join("");

    // -- graphe CA / EBITDA / Résultat net --
    var max = Math.max.apply(null, fin.map(function (f) { return Math.max(f.ca, f.ebitda, f.resultat_net); }));
    function bar(cls, val, label) {
      return '<div class="fc-bar ' + cls + '" data-h="' + (Math.max(val, 0) / max * 100) +
        '" title="' + label + " " + eur.format(val) + '"></div>';
    }
    var bars = fin.map(function (f) {
      return '<div class="fc-year"><div class="fc-bars">' +
        bar("ca", f.ca, "CA") + bar("ebitda", f.ebitda, "EBITDA") + bar("net", f.resultat_net, "Rés. net") +
        '</div><div class="fc-x">' + f.year + "</div></div>";
    }).join("");

    return '<div class="panel">' +
      '<div class="p-title">Situation financière <span class="cnt">' + fin.length + " exercice" + (fin.length > 1 ? "s" : "") +
      " · comptes annuels NBB ≥ 2021</span></div>" +
      '<p class="p-sub">Tous les KPI dérivables des codes comptables BNB. Tuiles et ratios = dernier exercice (' + last.year + ").</p>" +
      '<div class="kpis">' + tilesHTML + "</div>" +
      groups +
      '<div class="sub-title">Détail pluriannuel</div>' +
      '<div class="fin-table-wrap"><table class="fin-table"><thead>' + thead + "</thead><tbody>" + tbody + "</tbody></table></div>" +
      '<div class="fin-chart">' + bars + "</div>" +
      '<div class="fin-legend"><span class="li"><span class="sw" style="background:var(--gold)"></span>Chiffre d\'affaires</span>' +
      '<span class="li"><span class="sw" style="background:var(--nbb)"></span>EBITDA</span>' +
      '<span class="li"><span class="sw" style="background:var(--done)"></span>Résultat net</span></div>' +
      "</div>";
  }

  function animateFin() {
    requestAnimationFrame(function () {
      requestAnimationFrame(function () {
        document.querySelectorAll(".fc-bar").forEach(function (b) { b.style.height = b.getAttribute("data-h") + "%"; });
      });
    });
  }

  /* ---------- documents ---------- */
  function documentsHTML(c) {
    var docs = c.documents || [];
    if (!docs.length) return '<div class="panel"><div class="p-title">Documents liés</div><p class="p-sub">Aucun document Bronze pour cette entreprise.</p></div>';
    var groups = ["nbb", "ejustice", "notaire"].map(function (src) {
      var list = docs.filter(function (d) { return d.source === src; });
      if (!list.length) return "";
      var rows = list.sort(function (a, b) { return (b.year || 0) - (a.year || 0); }).map(function (d) {
        var canView = true;
        return '<div class="doc-row">' +
          '<div class="doc-year">' + esc(d.year || "—") + '</div>' +
          '<div><div class="doc-title">' + esc(d.title || d.ref) + '</div>' +
          '<div class="doc-ref">' + esc(d.kind) + ' · ' + esc(d.ref) + '</div></div>' +
          '<span class="pill ' + statusCls(d.status) + '">' + esc(d.status) + '</span>' +
          '<button class="btn" ' + (canView ? "" : "disabled") +
          ' data-bce="' + esc(c.bce) + '" data-source="' + esc(d.source) + '" data-ref="' + esc(d.ref) +
          '" data-title="' + esc(d.title || d.ref) + '" data-meta="' + esc((d.hdfs_path || "")) + '">Voir</button>' +
          '</div>';
      }).join("");
      return '<div class="doc-group"><h4><span class="src-dot" style="background:var(--' + src + ')"></span>' +
        SRC_LABELS[src] + ' <span class="cnt mono" style="color:var(--muted);font-weight:500">(' + list.length + ')</span></h4>' +
        '<div class="doc-list">' + rows + '</div></div>';
    }).join("");
    return '<div class="panel"><div class="p-title">Documents Bronze liés <span class="cnt">' + docs.length + '</span></div>' +
      '<p class="p-sub">Fichiers déposés sur HDFS par le pipeline d\'ingestion.</p>' +
      '<div class="doc-groups">' + groups + '</div></div>';
  }

  function bindDocButtons() {
    document.querySelectorAll(".doc-row .btn").forEach(function (b) {
      b.addEventListener("click", function () {
        openDoc(b.dataset.bce, b.dataset.source, b.dataset.ref, b.dataset.title, b.dataset.meta);
      });
    });
  }

  /* ---------- modal viewer ---------- */
  function openDoc(bce, source, ref, title, meta) {
    var url = "/api/document?bce=" + encodeURIComponent(bce) +
      "&source=" + encodeURIComponent(source) + "&ref=" + encodeURIComponent(ref);
    state.lastFocus = document.activeElement;  // pour restaurer le focus à la fermeture
    el("modal-title").textContent = title;
    el("modal-sub").textContent = source + " · " + (meta || ref);
    el("modal-open").href = url;
    el("modal-frame").src = url;
    el("modal").hidden = false;
    document.body.style.overflow = "hidden";
    var closeBtn = el("modal").querySelector("[data-close].btn");
    if (closeBtn) closeBtn.focus();
  }
  function closeModal() {
    el("modal").hidden = true;
    el("modal-frame").src = "about:blank";
    document.body.style.overflow = "";
    if (state.lastFocus && state.lastFocus.focus) state.lastFocus.focus();  // rend le focus au déclencheur
    state.lastFocus = null;
  }
  function focusables() {
    return Array.prototype.filter.call(
      el("modal").querySelectorAll('a[href], button:not([disabled]), iframe, [tabindex]:not([tabindex="-1"])'),
      function (x) { return x.offsetParent !== null; });
  }
  document.querySelectorAll("[data-close]").forEach(function (x) { x.addEventListener("click", closeModal); });
  document.addEventListener("keydown", function (e) {
    if (el("modal").hidden) return;
    if (e.key === "Escape") { closeModal(); return; }
    if (e.key === "Tab") {  // piège de focus dans la modale
      var f = focusables();
      if (!f.length) return;
      var first = f[0], last = f[f.length - 1];
      if (e.shiftKey && document.activeElement === first) { e.preventDefault(); last.focus(); }
      else if (!e.shiftKey && document.activeElement === last) { e.preventDefault(); first.focus(); }
    }
  });

  /* ---------- boot ---------- */
  runSearch();
})();
