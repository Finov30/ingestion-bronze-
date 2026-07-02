# BCE Frontend (Vite + React + Redux Toolkit)

Frontend for the Belgian-company (BCE) data pipeline — Jour 3. It consumes the
FastAPI backend and renders a search view plus a company fiche (general info,
financial ratios, a financial Sankey, dirigeants, and statutes streamed over SSE).

## Requirements

- Node v24, npm 11 (available in this environment)

## Install & run

```bash
npm install
npm run dev        # dev server on http://localhost:5173
```

Production build (this is the acceptance check):

```bash
npm install
npm run build      # emits dist/
npm run preview    # optional: serve the built dist/
```

## Configuration

The API base URL comes from the Vite env var `VITE_API_URL`
(default `http://localhost:8000`):

```bash
VITE_API_URL=http://localhost:8000 npm run dev
# or for a build:
VITE_API_URL=https://api.example.com npm run build
```

## API endpoints consumed

- `GET /api/health`
- `GET /api/search?q=<name|bce>&limit=20`
- `GET /api/enterprise/{bce}`
- `GET /api/enterprise/{bce}/dirigeants`
- `GET /api/enterprise/{bce}/statutes/stream` (Server-Sent Events)

## Structure

```
index.html
vite.config.js
src/
  main.jsx                 # entry: Provider + BrowserRouter
  App.jsx                  # routes: / and /enterprise/:bce
  store.js                 # Redux Toolkit store (4 slices)
  api.js                   # API_URL + apiGet helper
  index.css                # styling
  features/
    searchSlice.js         # createAsyncThunk GET /api/search
    enterpriseSlice.js     # createAsyncThunk GET /api/enterprise/{bce}
    dirigeantsSlice.js     # createAsyncThunk GET .../dirigeants
    statutesSlice.js       # SSE-driven state (EventSource in component)
  views/
    SearchView.jsx         # live search bar + results list
    EnterpriseView.jsx     # company fiche
  components/
    RatiosTable.jsx        # gold.years -> ratios table
    FinancialSankey.jsx    # custom-SVG Sankey (CA -> Marge brute -> Resultat net), year <select>
    DirigeantsList.jsx     # dirigeants (nom + qualites)
    StatutesList.jsx       # progressive SSE list with spinner
```

## Notes

- Plain JavaScript (no TypeScript) to avoid type friction.
- The Sankey is a small custom SVG (no external chart lib) so the build is
  always clean and dependency-free.
- Statutes are loaded progressively via `EventSource`; each `data:` message is
  appended immediately, a spinner shows while streaming, and it hides on the
  `done` or `error` event.
