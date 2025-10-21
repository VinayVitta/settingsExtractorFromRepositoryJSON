from fastapi import APIRouter, UploadFile, File, Request, Form
from fastapi.responses import JSONResponse, FileResponse
from pathlib import Path
import uuid
import os
from ..services.runner import run_extraction
from helpers.logger_config import setup_logger


router = APIRouter()

UPLOAD_DIR = Path("backend/temp_uploads")
UPLOAD_DIR.mkdir(parents=True, exist_ok=True)

# Loggers
backend_logger = setup_logger(__name__)
ui_logger = setup_logger("ui_files", ui=True)


# ---------------------------
# Download endpoint
# ---------------------------
@router.get("/download/{file_path:path}")
async def download_file(file_path: str, request: Request):
    """
    Download a file by relative path.
    Logs both backend and UI activity.
    """
    client_ip = request.client.host if request.client else "unknown"
    full_path = UPLOAD_DIR / file_path

    ui_logger.info(f"Download request from {client_ip}: {file_path}")

    if full_path.exists() and full_path.is_file():
        ui_logger.info(f"File found: {full_path}")
        backend_logger.info(f"Serving file to {client_ip}: {full_path}")
        return FileResponse(full_path, filename=full_path.name, media_type="application/octet-stream")

    ui_logger.warning(f"File not found: {full_path}")
    backend_logger.warning(f"Client {client_ip} requested missing file: {full_path}")
    return JSONResponse(status_code=404, content={"error": f"File not found: {file_path}"})


# ---------------------------
# Upload + Run endpoint
# ---------------------------
@router.post("/run")
async def upload_and_run(
    request: Request,
    json_files: list[UploadFile] = File(...),
    tsv_file: UploadFile = File(...),
    include_all_states: bool = Form(False)  #  new checkbox param, default is False
):
    """
    Accept multiple JSON files + one TSV file.
    Runs extraction and returns relative paths for download.
    """
    client_ip = request.client.host if request.client else "unknown"
    temp_id = str(uuid.uuid4())
    temp_folder = UPLOAD_DIR / temp_id
    temp_folder.mkdir(parents=True, exist_ok=True)

    ui_logger.info(f"Upload request from {client_ip} — temp folder: {temp_folder}")
    backend_logger.info(f"Saving uploaded files to {temp_folder}")
    backend_logger.info(f"Include all states: {include_all_states}")

    saved_jsons = []

    try:
        # Save JSON files
        for json_file in json_files:
            path = temp_folder / json_file.filename
            data = await json_file.read()
            with open(path, "wb") as f:
                f.write(data)
            saved_jsons.append(path)
            ui_logger.info(f"Saved JSON file: {path} ({len(data)} bytes)")

        # Save TSV file
        tsv_path = temp_folder / tsv_file.filename
        data = await tsv_file.read()
        with open(tsv_path, "wb") as f:
            f.write(data)
        ui_logger.info(f"Saved TSV file: {tsv_path} ({len(data)} bytes)")

        # Run extraction
        backend_logger.info(f"Starting extraction for {client_ip} in {temp_folder}")
        output_files = run_extraction(saved_jsons, tsv_path, temp_folder, include_all_states=include_all_states)
        backend_logger.info(f"Extraction completed for {client_ip}")

        # Convert to relative paths
        def relative_paths(obj):
            if isinstance(obj, Path):
                return str(obj.relative_to(UPLOAD_DIR))
            elif isinstance(obj, list):
                return [relative_paths(i) for i in obj]
            elif isinstance(obj, dict):
                return {k: relative_paths(v) for k, v in obj.items()}
            return obj

        result = {"outputs": relative_paths(output_files)}

        ui_logger.info(f"Extraction finished successfully for {client_ip}")
        return JSONResponse(result)

    except Exception as e:
        ui_logger.exception(f"Error during upload/run for {client_ip}: {e}")
        backend_logger.exception(f"Error processing upload/run for {client_ip}: {e}")
        return JSONResponse(status_code=500, content={"error": str(e)})
