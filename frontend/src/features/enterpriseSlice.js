import { createSlice, createAsyncThunk } from '@reduxjs/toolkit';
import { apiGet } from '../api';

export const fetchEnterprise = createAsyncThunk(
  'enterprise/fetch',
  async (bce) => {
    return apiGet(`/api/enterprise/${encodeURIComponent(bce)}`);
  }
);

const enterpriseSlice = createSlice({
  name: 'enterprise',
  initialState: {
    data: null, // { enterprise_number, silver, gold }
    status: 'idle',
    error: null,
  },
  reducers: {
    clearEnterprise(state) {
      state.data = null;
      state.status = 'idle';
      state.error = null;
    },
  },
  extraReducers: (builder) => {
    builder
      .addCase(fetchEnterprise.pending, (state) => {
        state.status = 'loading';
        state.error = null;
        state.data = null;
      })
      .addCase(fetchEnterprise.fulfilled, (state, action) => {
        state.status = 'succeeded';
        state.data = action.payload;
      })
      .addCase(fetchEnterprise.rejected, (state, action) => {
        state.status = 'failed';
        state.error = action.error.message;
      });
  },
});

export const { clearEnterprise } = enterpriseSlice.actions;
export default enterpriseSlice.reducer;
