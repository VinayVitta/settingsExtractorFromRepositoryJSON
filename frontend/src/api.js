import axios from "axios";
const apiBase = process.env.REACT_APP_API_BASE_URL;
const API_BASE = apiBase // ✅ same IP as your backend

export const api = axios.create({
  baseURL: API_BASE,
});
