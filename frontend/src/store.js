import { configureStore } from '@reduxjs/toolkit';
import searchReducer from './features/searchSlice';
import enterpriseReducer from './features/enterpriseSlice';
import dirigeantsReducer from './features/dirigeantsSlice';
import statutesReducer from './features/statutesSlice';

export const store = configureStore({
  reducer: {
    search: searchReducer,
    enterprise: enterpriseReducer,
    dirigeants: dirigeantsReducer,
    statutes: statutesReducer,
  },
});

export default store;
