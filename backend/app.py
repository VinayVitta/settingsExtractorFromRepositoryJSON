import os
import sys
from pathlib import Path
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from dotenv import load_dotenv

# --- Add backend root to sys.path if needed ---
BASE_DIR = Path(__file__).resolve().parent
sys.path.append(str(BASE_DIR))  # ensures helpers and routers are importable

# --- Load environment variables ---
ENV_PATH = BASE_DIR / ".env"
load_dotenv(dotenv_path=ENV_PATH)

# --- Logger ---
from helpers.logger_config import setup_logger
logger = setup_logger("app")

# --- FastAPI App ---
app = FastAPI(title="QDI - PS: Replicate Health Check")

# --- CORS configuration ---
FRONTEND_ORIGINS = os.getenv("FRONTEND_ORIGINS", "http://localhost:3000")
origins = [origin.strip() for origin in FRONTEND_ORIGINS.split(",")]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# --- Import routers ---
from backend.routers import extract, download, endpoints_info

app.include_router(extract.router, prefix="/extract", tags=["extract"])
app.include_router(download.router, prefix="/download", tags=["download"])
app.include_router(endpoints_info.router, tags=["Info"])

# --- Serve frontend static files ---
STATIC_FILES_DIR = BASE_DIR.parent / "frontend" / "build"
INDEX_FILE = STATIC_FILES_DIR / "index.html"

if not INDEX_FILE.exists():
    logger.error(f"Critical error: index.html not found in {STATIC_FILES_DIR}")
    raise FileNotFoundError(f"index.html missing. Build frontend first.")

app.mount(
    "/",
    StaticFiles(directory=STATIC_FILES_DIR, html=True),
    name="static_files"
)

# --- Startup / Shutdown events ---
@app.on_event("startup")
async def startup_event():
    logger.info(" Backend service started.")

@app.on_event("shutdown")
async def shutdown_event():
    logger.info(" Backend service stopped.")
