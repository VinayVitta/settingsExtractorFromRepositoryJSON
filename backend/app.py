from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from routers import extract, download

app = FastAPI(title="QEM Repository Extractor API")

# Allow frontend to communicate
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # or your frontend domain
    allow_methods=["*"],
    allow_headers=["*"]
)

app.include_router(extract.router, prefix="/extract", tags=["extract"])
app.include_router(download.router, prefix="/download", tags=["download"])
