import { createSlice, createAsyncThunk } from '@reduxjs/toolkit';
import { apiGet } from '../api';

export const doSearch = createAsyncThunk(
  'search/doSearch',
  async ({ q, limit = 20 }) => {
    const query = encodeURIComponent(q || '');
    const data = await apiGet(`/api/search?q=${query}&limit=${limit}`);
    return data.results || [];
  }
);

const searchSlice = createSlice({
  name: 'search',
  initialState: {
    query: '',
    results: [],
    status: 'idle', // idle | loading | succeeded | failed
    error: null,
  },
  reducers: {
    setQuery(state, action) {
      state.query = action.payload;
    },
    clearResults(state) {
      state.results = [];
      state.status = 'idle';
    },
  },
  extraReducers: (builder) => {
    builder
      .addCase(doSearch.pending, (state) => {
        state.status = 'loading';
        state.error = null;
      })
      .addCase(doSearch.fulfilled, (state, action) => {
        state.status = 'succeeded';
        state.results = action.payload;
      })
      .addCase(doSearch.rejected, (state, action) => {
        state.status = 'failed';
        state.error = action.error.message;
      });
  },
});

export const { setQuery, clearResults } = searchSlice.actions;
export default searchSlice.reducer;
