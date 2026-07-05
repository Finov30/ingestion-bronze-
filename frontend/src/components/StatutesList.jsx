import React, { useEffect, useRef } from 'react';
import { useDispatch, useSelector } from 'react-redux';
import { API_URL } from '../api';
import {
  streamStarted,
  statuteReceived,
  streamDone,
  streamError,
  clearStatutes,
} from '../features/statutesSlice';

// Progressive load of statutes over SSE. Each "data:" line is appended on
// arrival; a spinner shows while the stream is open and hides on 'done'/'error'.
export default function StatutesList({ bce }) {
  const dispatch = useDispatch();
  const { list, streaming, count, error } = useSelector((s) => s.statutes);
  const esRef = useRef(null);

  useEffect(() => {
    if (!bce) return undefined;

    dispatch(streamStarted());
    const url = `${API_URL}/api/enterprise/${encodeURIComponent(
      bce
    )}/statutes/stream`;
    const es = new EventSource(url);
    esRef.current = es;

    // Default (unnamed) SSE messages: one statute each.
    es.onmessage = (evt) => {
      try {
        dispatch(statuteReceived(JSON.parse(evt.data)));
      } catch (e) {
        // ignore malformed line
      }
    };

    // Server signals completion with a named "done" event.
    es.addEventListener('done', (evt) => {
      let payload = {};
      try {
        payload = JSON.parse(evt.data);
      } catch (e) {
        payload = {};
      }
      dispatch(streamDone(payload));
      es.close();
    });

    // Named "error" event from the scraper, or a connection error.
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
      dispatch(streamError({ message: msg }));
      es.close();
    });

    return () => {
      es.close();
      dispatch(clearStatutes());
    };
  }, [bce, dispatch]);

  return (
    <div className="card">
      <h2>
        Statuts / actes{' '}
        {streaming && <span className="spinner" />}
        {!streaming && count !== null && (
          <span className="tag">{count} document(s)</span>
        )}
      </h2>
      {error && <p className="error">Erreur: {error}</p>}
      {!streaming && list.length === 0 && !error && (
        <p className="muted">Aucun statut.</p>
      )}
      {list.map((s, i) => (
        <div
          key={s.documentId || i}
          style={{
            borderBottom: '1px solid var(--border)',
            padding: '8px 0',
          }}
        >
          <strong>{s.document || '(document)'}</strong>
          <div className="muted" style={{ marginTop: 2 }}>
            {[s.date, s.notaire, s.statut].filter(Boolean).join(' — ')}
          </div>
        </div>
      ))}
    </div>
  );
}
