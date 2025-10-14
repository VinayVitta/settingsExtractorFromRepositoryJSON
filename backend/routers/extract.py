from fastapi import APIRouter, UploadFile, File
from fastapi.responses import JSONResponse, FileResponse
import os
from pathlib import Path
import uuid
from services.runner import run_extraction  # make sure this exists

router = APIRouter()

UPLOAD_DIR = Path("backend/temp_uploads")
UPLOAD_DIR.mkdir(parents=True, exist_ok=True)

# ---------------------------
# Download endpoint
# ---------------------------
@router.get("/download/{file_path:path}")
async def download_file(file_path: str):
    """
    Download a file by relative path
    """
    full_path = UPLOAD_DIR / file_path
    if full_path.exists():
        return FileResponse(full_path, filename=full_path.name, media_type='application/octet-stream')
    return {"error": f"File not found: {full_path}"}


# ---------------------------
# Upload + Run endpoint
# ---------------------------
@router.post("/run")
async def upload_and_run(json_files: list[UploadFile] = File(...), tsv_file: UploadFile = File(...)):
    """
    Accept multiple JSON files + one TSV file.
    Runs extraction and returns relative paths for download.
    """
    temp_id = str(uuid.uuid4())
    temp_folder = UPLOAD_DIR / temp_id
    temp_folder.mkdir(parents=True, exist_ok=True)

    # Save JSON files
    saved_jsons = []
    for json_file in json_files:
        path = temp_folder / json_file.filename
        with open(path, "wb") as f:
            f.write(await json_file.read())
        saved_jsons.append(path)

    # Save TSV file
    tsv_path = temp_folder / tsv_file.filename
    with open(tsv_path, "wb") as f:
        f.write(await tsv_file.read())

    # Run extraction
    output_files = run_extraction(saved_jsons, tsv_path, temp_folder)

    # Convert all output paths to **relative** paths
    def relative_paths(obj):
        if isinstance(obj, Path):
            return str(obj.relative_to(UPLOAD_DIR))
        elif isinstance(obj, list):
            return [relative_paths(i) for i in obj]
        elif isinstance(obj, dict):
            return {k: relative_paths(v) for k, v in obj.items()}
        return obj

    return JSONResponse({"outputs": relative_paths(output_files)})
