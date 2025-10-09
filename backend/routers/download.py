from fastapi import APIRouter
from fastapi.responses import FileResponse
import os

router = APIRouter()

@router.get("/{file_path:path}")
async def download_file(file_path: str):
    """
    Download any generated output file by path
    """
    full_path = os.path.join("backend/temp_uploads", file_path)
    if os.path.exists(full_path):
        return FileResponse(full_path, filename=os.path.basename(full_path))
    return {"error": "File not found"}
