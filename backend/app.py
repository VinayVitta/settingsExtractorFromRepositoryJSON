from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from routers import extract, download

# --- NEW IMPORTS for ABSOLUTE PATH FIX ---
from pathlib import Path
import os
import sys  # Added for graceful exit if path fails

app = FastAPI(title="QDI - PS: Replicate Health Check")

# --- ABSOLUTE PATH FIX & VALIDATION ---
# 1. Determine the directory of the currently executing file (backend/)
BASE_DIR = Path(__file__).resolve().parent
# 2. Construct the absolute path to the 'frontend/build' folder
# This path is now immune to changes in the service's starting directory.
STATIC_FILES_DIR = BASE_DIR.parent / "frontend" / "build"

# --- CRITICAL PATH CHECK FOR SERVICE DEPLOYMENT ---
# We explicitly check for the required index.html file to ensure the frontend build ran.
INDEX_FILE = STATIC_FILES_DIR / "index.html"

if not INDEX_FILE.exists():
    # Print to standard error so the service manager may log this more easily
    print(f"CRITICAL ERROR: 'index.html' not found in static files directory: {STATIC_FILES_DIR}", file=sys.stderr)
    print("ACTION REQUIRED: Ensure you have run 'npm install' and 'npm run build' in the frontend directory on this server.", file=sys.stderr)
    # Raising an exception will often be logged more clearly by the service manager
    raise FileNotFoundError(f"Required file 'index.html' not found at: {INDEX_FILE}. Did you run the frontend build?")

# --- 1. CORS Configuration ---
# ✅ Allow frontend origins (local + network) for API calls (e.g., during dev)
origins = [
    "http://localhost:3000",
    "http://127.0.0.1:3000",
    "http://192.168.56.1:3000"
]

# Apply the CORS Middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# --- 2. API Routers ---
# These routes will handle requests starting with /extract and /download
app.include_router(extract.router, prefix="/extract", tags=["extract"])
app.include_router(download.router, prefix="/download", tags=["download"])

# --- 3. SERVE STATIC FRONTEND FILES (CRITICAL FIX) ---
# This must be the *last* route definition.
app.mount(
    "/",
    # We now use the calculated absolute path (STATIC_FILES_DIR)
    StaticFiles(directory=STATIC_FILES_DIR, html=True),
    name="static_files"
)
