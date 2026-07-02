import React from 'react';

function fmt(v, digits = 0) {
  if (v === null || v === undefined || Number.isNaN(v)) return '—';
  return Number(v).toLocaleString('fr-BE', {
    minimumFractionDigits: digits,
    maximumFractionDigits: digits,
  });
}

function pct(v) {
  if (v === null || v === undefined || Number.isNaN(v)) return '—';
  return `${Number(v).toFixed(2)} %`;
}

// gold.years -> table year x {ca, marge_brute, marge_nette, roe, ratio_liquidite, taux_endettement}
export default function RatiosTable({ years }) {
  if (!years || years.length === 0) {
    return <p className="muted">Aucune donnée financière.</p>;
  }
  return (
    <div style={{ overflowX: 'auto' }}>
      <table>
        <thead>
          <tr>
            <th>Année</th>
            <th>CA</th>
            <th>Marge brute</th>
            <th>Marge nette</th>
            <th>ROE</th>
            <th>Ratio liquidité</th>
            <th>Taux endettement</th>
          </tr>
        </thead>
        <tbody>
          {years.map((y) => {
            const r = y.ratios || {};
            return (
              <tr key={y.year}>
                <td>{y.year}</td>
                <td>{fmt(y.ca)}</td>
                <td>{fmt(y.marge_brute)}</td>
                <td>{pct(r.marge_nette)}</td>
                <td>{pct(r.roe)}</td>
                <td>
                  {r.ratio_liquidite === null || r.ratio_liquidite === undefined
                    ? '—'
                    : Number(r.ratio_liquidite).toFixed(2)}
                </td>
                <td>{pct(r.taux_endettement)}</td>
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}
