import React, { useEffect, useRef } from 'react';
import { useDispatch, useSelector } from 'react-redux';
import { API_URL } from '../api';
import {
  dirStreamStarted,
  dirigeantReceived,
  dirStreamDone,
  dirStreamError,
  clearDirigeants,
} from '../features/dirigeantsSlice';

// Progressive load of dirigeants over SSE (like the statutes). Each "data:"
// line is one dirigeant, appended on arrival; a spinner shows while the stream
// is open and hides on 'done'/'error'. Scraped once server-side then cached.
export default function DirigeantsList({ bce }) {
  const dispatch = useDispatch();
  const { list, streaming, count, cached, error } = useSelector(
    (s) => s.dirigeants
  );
  const esRef = useRef(null);

  useEffect(() => {
    if (!bce) return undefined;

    dispatch(dirStreamStarted());
    const url = `${API_URL}/api/enterprise/${encodeURIComponent(
      bce
    )}/dirigeants/stream`;
    const es = new EventSource(url);
    esRef.current = es;

    es.onmessage = (evt) => {
      try {
        dispatch(dirigeantReceived(JSON.parse(evt.data)));
      } catch (e) {
        // ignore malformed line
      }
    };

    es.addEventListener('done', (evt) => {
      let payload = {};
      try {
        payload = JSON.parse(evt.data);
      } catch (e) {
        payload = {};
      }
      dispatch(dirStreamDone(payload));
      es.close();
    });

    es.addEventListener('error', (evt) => {
      let msg = 'Flux interrompu';
      if (evt && evt.data) {
        try {
          const p = JSON.parse(evt.data);
          if (p && p.message) msg = p.message;
        } catch (e) {
          /* keep default */
        }
      }
      dispatch(dirStreamError({ message: msg }));
      es.close();
    });

    return () => {
      es.close();
      dispatch(clearDirigeants());
    };
  }, [bce, dispatch]);

  return (
    <div className="card">
      <h2>
        Dirigeants{' '}
        {streaming && <span className="spinner" />}
        {cached && <span className="tag">cache</span>}
        {!streaming && count !== null && (
          <span className="tag">{count} dirigeant(s)</span>
        )}
      </h2>
      {error && <p className="error">Erreur: {error}</p>}
      {!streaming && list.length === 0 && !error && (
        <p className="muted">Aucun dirigeant trouvé.</p>
      )}
      {list.map((d, i) => (
        <div key={`${d.nom}-${i}`} style={{ marginBottom: 10 }}>
          <strong>{d.nom}</strong>
          <div>
            {(d.qualites || []).map((q, j) => (
              <span className="tag" key={j}>
                {q}
              </span>
            ))}
          </div>
        </div>
      ))}
    </div>
  );
}
