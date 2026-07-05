import { createSlice } from '@reduxjs/toolkit';

// Dirigeants are streamed over SSE (EventSource), like the statutes. The
// component drives the EventSource lifecycle; this slice only accumulates the
// received dirigeants and the streaming status. Each "data:" event appends one
// dirigeant via dirigeantReceived; the stream ends on 'done' or 'error'.
const dirigeantsSlice = createSlice({
  name: 'dirigeants',
  initialState: {
    list: [],
    streaming: false,
    count: null,
    cached: false,
    error: null,
  },
  reducers: {
    dirStreamStarted(state) {
      state.list = [];
      state.streaming = true;
      state.count = null;
      state.cached = false;
      state.error = null;
    },
    dirigeantReceived(state, action) {
      state.list.push(action.payload);
    },
    dirStreamDone(state, action) {
      state.streaming = false;
      state.count =
        action.payload && typeof action.payload.count === 'number'
          ? action.payload.count
          : state.list.length;
      state.cached = !!(action.payload && action.payload.cached);
    },
    dirStreamError(state, action) {
      state.streaming = false;
      state.error = (action.payload && action.payload.message) || 'stream error';
    },
    clearDirigeants(state) {
      state.list = [];
      state.streaming = false;
      state.count = null;
      state.cached = false;
      state.error = null;
    },
  },
});

export const {
  dirStreamStarted,
  dirigeantReceived,
  dirStreamDone,
  dirStreamError,
  clearDirigeants,
} = dirigeantsSlice.actions;
export default dirigeantsSlice.reducer;
