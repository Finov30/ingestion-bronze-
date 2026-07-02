// Central place for the API base URL and small fetch helpers.
export const API_URL =
  (import.meta.env && import.meta.env.VITE_API_URL) || 'http://localhost:8000';

export async function apiGet(path) {
  const res = await fetch(`${API_URL}${path}`);
  if (!res.ok) {
    throw new Error(`HTTP ${res.status} on ${path}`);
  }
  return res.json();
}
