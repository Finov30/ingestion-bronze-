import React from 'react';
import { Routes, Route, Link } from 'react-router-dom';
import SearchView from './views/SearchView';
import EnterpriseView from './views/EnterpriseView';

export default function App() {
  return (
    <>
      <header className="appbar">
        <Link to="/" className="brand">
          BCE · Fiches entreprises
        </Link>
        <span className="muted">Données belges — Bronze / Silver / Gold</span>
      </header>
      <Routes>
        <Route path="/" element={<SearchView />} />
        <Route path="/enterprise/:bce" element={<EnterpriseView />} />
        <Route
          path="*"
          element={
            <div className="container">
              <p>Page introuvable. <Link to="/">Retour</Link></p>
            </div>
          }
        />
      </Routes>
    </>
  );
}
