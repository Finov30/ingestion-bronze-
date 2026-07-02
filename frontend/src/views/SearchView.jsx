import React, { useEffect, useRef } from 'react';
import { useDispatch, useSelector } from 'react-redux';
import { useNavigate } from 'react-router-dom';
import { doSearch, setQuery } from '../features/searchSlice';

export default function SearchView() {
  const dispatch = useDispatch();
  const navigate = useNavigate();
  const { query, results, status, error } = useSelector((s) => s.search);
  const debounceRef = useRef(null);

  // Live search: debounce input changes and query the backend.
  useEffect(() => {
    if (debounceRef.current) clearTimeout(debounceRef.current);
    const q = query.trim();
    if (!q) return;
    debounceRef.current = setTimeout(() => {
      dispatch(doSearch({ q, limit: 20 }));
    }, 300);
    return () => {
      if (debounceRef.current) clearTimeout(debounceRef.current);
    };
  }, [query, dispatch]);

  const onSubmit = (e) => {
    e.preventDefault();
    const q = query.trim();
    if (q) dispatch(doSearch({ q, limit: 20 }));
  };

  return (
    <div className="container">
      <div className="card">
        <h2>Recherche d'entreprise</h2>
        <form onSubmit={onSubmit}>
          <input
            type="search"
            placeholder="Nom de l'entreprise ou numéro BCE (ex: 0878065378)"
            value={query}
            autoFocus
            onChange={(e) => dispatch(setQuery(e.target.value))}
          />
        </form>
        <div className="row" style={{ marginTop: 8 }}>
          {status === 'loading' && <span className="spinner" />}
          {status === 'failed' && <span className="error">Erreur: {error}</span>}
          {status === 'succeeded' && (
            <span className="muted">{results.length} résultat(s)</span>
          )}
        </div>
      </div>

      <div className="card">
        {results.length === 0 && status === 'succeeded' && (
          <p className="muted">Aucun résultat.</p>
        )}
        {results.map((r) => (
          <button
            key={r.enterprise_number}
            className="result-item"
            onClick={() => navigate(`/enterprise/${r.enterprise_number}`)}
          >
            <span className="en">{r.enterprise_number}</span>
            <strong>{r.denomination || '(sans dénomination)'}</strong>
            <div className="muted" style={{ marginTop: 4 }}>
              {[r.juridical_form_label, r.status_label]
                .filter(Boolean)
                .join(' — ')}
            </div>
          </button>
        ))}
      </div>
    </div>
  );
}
