import axios from "axios";

const API_BASE = "http://192.168.56.1:8000"; // ✅ same IP as your backend

export const api = axios.create({
  baseURL: API_BASE,
});
