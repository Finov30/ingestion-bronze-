import React, { useEffect } from 'react';
import { useDispatch, useSelector } from 'react-redux';
import { useParams, Link } from 'react-router-dom';
import { fetchEnterprise, clearEnterprise } from '../features/enterpriseSlice';
import RatiosTable from '../components/RatiosTable';
import FinancialSankey from '../components/FinancialSankey';
import DirigeantsList from '../components/DirigeantsList';
import StatutesList from '../components/StatutesList';

function Address({ address }) {
  if (!address) return <span className="muted">—</span>;
  const line = [
    [address.StreetFR, address.HouseNumber].filter(Boolean).join(' '),
    [address.Zipcode, address.MunicipalityFR].filter(Boolean).join(' '),
  ]
    .filter(Boolean)
    .join(', ');
  return <span>{line || '—'}</span>;
}

export default function EnterpriseView() {
  const { bce } = useParams();
  const dispatch = useDispatch();
  const { data, status, error } = useSelector((s) => s.enterprise);

  useEffect(() => {
    if (bce) dispatch(fetchEnterprise(bce));
    return () => dispatch(clearEnterprise());
  }, [bce, dispatch]);

  if (status === 'loading') {
    return (
      <div className="container">
        <span className="spinner" /> Chargement…
      </div>
    );
  }
  if (status === 'failed') {
    return (
      <div className="container">
        <p className="error">Erreur: {error}</p>
        <Link to="/">← Retour à la recherche</Link>
      </div>
    );
  }
  if (!data) return null;

  const silver = data.silver;
  const gold = data.gold;

  return (
    <div className="container">
      <p>
        <Link to="/">← Retour à la recherche</Link>
      </p>

      <div className="card">
        <h2>Informations générales</h2>
        <h3 style={{ marginTop: 0 }}>
          {(silver && silver.denomination_principale) ||
            data.enterprise_number}
        </h3>
        <dl className="kv">
          <dt>Numéro BCE</dt>
          <dd style={{ fontFamily: 'ui-monospace, monospace' }}>
            {data.enterprise_number}
          </dd>
          <dt>Statut</dt>
          <dd>{(silver && silver.StatusLabel) || '—'}</dd>
          <dt>Forme juridique</dt>
          <dd>{(silver && silver.JuridicalFormLabel) || '—'}</dd>
          <dt>Date de début</dt>
          <dd>{(silver && silver.StartDate) || '—'}</dd>
          <dt>Adresse</dt>
          <dd>
            <Address address={silver && silver.address} />
          </dd>
        </dl>
      </div>

      <div className="card">
        <h2>Activités (NACE)</h2>
        {silver && silver.activities && silver.activities.length > 0 ? (
          <ul style={{ margin: 0, paddingLeft: 18 }}>
            {silver.activities.map((a, i) => (
              <li key={`${a.NaceCode}-${i}`}>
                <span
                  style={{
                    fontFamily: 'ui-monospace, monospace',
                    color: 'var(--accent)',
                  }}
                >
                  {a.NaceCode}
                </span>{' '}
                {a.NaceLabel}{' '}
                {a.Classification && (
                  <span className="tag">{a.Classification}</span>
                )}
              </li>
            ))}
          </ul>
        ) : (
          <p className="muted">Aucune activité.</p>
        )}
      </div>

      <div className="card">
        <h2>
          Ratios financiers{' '}
          {gold && gold.schema_type && (
            <span className="tag">{gold.schema_type}</span>
          )}
        </h2>
        <RatiosTable years={gold && gold.years} />
        {gold && gold.last_updated && (
          <p className="muted" style={{ marginTop: 8 }}>
            Mis à jour: {gold.last_updated}
          </p>
        )}
      </div>

      <div className="card">
        <h2>Flux financier</h2>
        <FinancialSankey years={gold && gold.years} />
      </div>

      <DirigeantsList bce={data.enterprise_number} />
      <StatutesList bce={data.enterprise_number} />
    </div>
  );
}
