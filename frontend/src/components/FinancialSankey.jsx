import React, { useState } from 'react';

// Lightweight custom-SVG Sankey with three fixed nodes:
//   CA -> Marge brute -> Resultat net
// for a year selected via a <select> over gold.years. No external chart lib,
// so it always builds cleanly.

function fmt(v) {
  if (v === null || v === undefined || Number.isNaN(v)) return '—';
  return Number(v).toLocaleString('fr-BE', { maximumFractionDigits: 0 });
}

export default function FinancialSankey({ years }) {
  const [idx, setIdx] = useState(years && years.length ? years.length - 1 : 0);

  if (!years || years.length === 0) {
    return <p className="muted">Aucune donnée pour le graphe.</p>;
  }

  const y = years[Math.min(idx, years.length - 1)];
  const nodes = [
    { label: "Chiffre d'affaires", value: Number(y.ca) || 0, color: '#38bdf8' },
    { label: 'Marge brute', value: Number(y.marge_brute) || 0, color: '#818cf8' },
    { label: 'Résultat net', value: Number(y.resultat_net) || 0, color: '#34d399' },
  ];

  const W = 720;
  const H = 260;
  const pad = 20;
  const nodeW = 24;
  const gap = (W - pad * 2 - nodeW * 3) / 2;
  const maxVal = Math.max(1, ...nodes.map((n) => Math.abs(n.value)));
  const maxBarH = H - pad * 2 - 24;

  const xs = [pad, pad + nodeW + gap, pad + (nodeW + gap) * 2];
  const heights = nodes.map((n) =>
    Math.max(2, (Math.abs(n.value) / maxVal) * maxBarH)
  );
  const ys = heights.map((h) => pad + 12 + (maxBarH - h) / 2);

  // Ribbon between node i and i+1 (uses the smaller of the two heights).
  const ribbon = (i) => {
    const x0 = xs[i] + nodeW;
    const x1 = xs[i + 1];
    const h = Math.min(heights[i], heights[i + 1]);
    const yc0 = ys[i] + heights[i] / 2;
    const yc1 = ys[i + 1] + heights[i + 1] / 2;
    const topA = yc0 - h / 2;
    const botA = yc0 + h / 2;
    const topB = yc1 - h / 2;
    const botB = yc1 + h / 2;
    const mx = (x0 + x1) / 2;
    return `M ${x0} ${topA} C ${mx} ${topA}, ${mx} ${topB}, ${x1} ${topB}
            L ${x1} ${botB} C ${mx} ${botB}, ${mx} ${botA}, ${x0} ${botA} Z`;
  };

  return (
    <div>
      <div className="row" style={{ marginBottom: 12 }}>
        <label className="muted" htmlFor="sankey-year">
          Année&nbsp;
        </label>
        <select
          id="sankey-year"
          value={idx}
          onChange={(e) => setIdx(Number(e.target.value))}
          style={{
            background: 'var(--panel-2)',
            color: 'var(--text)',
            border: '1px solid var(--border)',
            borderRadius: 6,
            padding: '6px 10px',
          }}
        >
          {years.map((yy, i) => (
            <option key={yy.year} value={i}>
              {yy.year}
            </option>
          ))}
        </select>
      </div>

      <div style={{ overflowX: 'auto' }}>
        <svg width={W} height={H} role="img" aria-label="Flux financier">
          {[0, 1].map((i) => (
            <path
              key={i}
              d={ribbon(i)}
              fill={nodes[i].color}
              opacity="0.35"
            />
          ))}
          {nodes.map((n, i) => (
            <g key={n.label}>
              <rect
                x={xs[i]}
                y={ys[i]}
                width={nodeW}
                height={heights[i]}
                rx="3"
                fill={n.color}
              />
              <text
                x={xs[i] + nodeW / 2}
                y={ys[i] - 6}
                fill="var(--text)"
                fontSize="12"
                textAnchor="middle"
              >
                {n.label}
              </text>
              <text
                x={xs[i] + nodeW / 2}
                y={ys[i] + heights[i] + 16}
                fill="var(--muted)"
                fontSize="11"
                textAnchor="middle"
              >
                {fmt(n.value)}
              </text>
            </g>
          ))}
        </svg>
      </div>
    </div>
  );
}
