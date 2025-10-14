from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from routers import extract, download

app = FastAPI(title="QDI - PS: Replicate Health Check")

# ✅ Allow frontend origins (local + network)
origins = [
    "http://localhost:3000",
    "http://127.0.0.1:3000",
    "http://192.168.56.1:3000"
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Allow frontend to communicate
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # or your frontend domain
    allow_methods=["*"],
    allow_headers=["*"]
)

app.include_router(extract.router, prefix="/extract", tags=["extract"])
app.include_router(download.router, prefix="/download", tags=["download"])
