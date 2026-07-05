import { createSlice } from '@reduxjs/toolkit';

// The SSE (EventSource) lifecycle is driven from the component; this slice
// only stores the accumulated statutes and streaming status. Each incoming
// "data:" event is appended via statuteReceived; the stream ends on 'done' or
// 'error'.
const statutesSlice = createSlice({
  name: 'statutes',
  initialState: {
    list: [],
    streaming: false,
    count: null,
    error: null,
  },
  reducers: {
    streamStarted(state) {
      state.list = [];
      state.streaming = true;
      state.count = null;
      state.error = null;
    },
    statuteReceived(state, action) {
      state.list.push(action.payload);
    },
    streamDone(state, action) {
      state.streaming = false;
      state.count =
        action.payload && typeof action.payload.count === 'number'
          ? action.payload.count
          : state.list.length;
    },
    streamError(state, action) {
      state.streaming = false;
      state.error = (action.payload && action.payload.message) || 'stream error';
    },
    clearStatutes(state) {
      state.list = [];
      state.streaming = false;
      state.count = null;
      state.error = null;
    },
  },
});

export const {
  streamStarted,
  statuteReceived,
  streamDone,
  streamError,
  clearStatutes,
} = statutesSlice.actions;
export default statutesSlice.reducer;
