from fastapi import APIRouter
from fastapi.responses import FileResponse
from pathlib import Path

router = APIRouter()

@router.get("/{file_path:path}")
async def download_file(file_path: str):
    """
    Download file to client browser
    """
    full_path = Path("backend/temp_uploads") / Path(file_path)  # combine with base dir

    if full_path.exists():
        # Force browser to download
        return FileResponse(full_path, filename=full_path.name, media_type='application/octet-stream')

    return {"error": f"File not found: {full_path}"}
