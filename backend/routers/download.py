from fastapi import APIRouter, Request, BackgroundTasks, Form
from fastapi.responses import FileResponse, JSONResponse
from pathlib import Path
from helpers.logger_config import setup_logger
import zipfile # <-- New import
import os # <-- New import

router = APIRouter()

# Create a dedicated UI logger
ui_logger = setup_logger("ui_download", ui=True)

# --- HELPER FUNCTION FOR CLEANUP ---
def cleanup_file(path: Path):
    """Removes the file after it's been sent."""
    try:
        os.remove(path)
        ui_logger.info(f"Cleaned up temporary file: {path}")
    except OSError as e:
        ui_logger.error(f"Error removing file {path}: {e}")

# --------------------------------------------------------------------------------
# 1. DOWNLOAD ALL FILES (NEW)
# --------------------------------------------------------------------------------
@router.post("/download_all")
async def download_all(
    # 1. Place non-default/injected parameters first (Request, BackgroundTasks)
    request: Request,
    background_tasks: BackgroundTasks,
    # 2. Place parameters with default values (Form, Query, Body, etc.) last
    outputs: list[str] = Form(...),
):
    """
    Accepts a list of relative file paths, zips them up, and sends the zip file.
    The temporary zip file is deleted after the response is sent.
    """
    base_dir = Path("backend/temp_uploads")
    client_ip = request.client.host if request.client else "unknown"

    if not outputs:
        return JSONResponse(status_code=400, content={"error": "No files provided for download."})

    # The zip file will be created in the first file's directory (e.g., the run-specific ID folder)
    # This keeps temp files organized
    if not outputs[0]:
         return JSONResponse(status_code=400, content={"error": "Invalid file path in outputs list."})

    # Get the unique run ID directory path (e.g., '6226e3f4-2371-4eb3-8951-5804bb32a0d1')
    # We assume all files in the list are from the same parent folder under temp_uploads.
    # This is a robust way to handle the example path structure.
    run_id_path = Path(outputs[0]).parent

    # Create a unique name for the zip file to avoid conflicts
    zip_filename = f"all_outputs_{Path(outputs[0]).parts[0]}.zip"
    # The full path to the zip file (e.g., backend/temp_uploads/run_id_path/all_outputs_ID.zip)
    zip_file_path = base_dir / run_id_path / zip_filename

    ui_logger.info(f"UI download all request from {client_ip} ({len(outputs)} files). Creating zip at: {zip_file_path}")

    try:
        with zipfile.ZipFile(zip_file_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
            for file_path_str in outputs:
                full_path = base_dir / Path(file_path_str)

                if full_path.exists() and full_path.is_file():
                    # We use Path(file_path_str).name as the archive name to keep it clean (e.g., 'task_summary_20251020_155245.csv')
                    # This avoids including the GUID folder structure inside the zip
                    zipf.write(full_path, arcname=Path(file_path_str).name)
                    ui_logger.debug(f"Added to zip: {file_path_str}")
                else:
                    ui_logger.warning(f"File not found (skipped in zip): {file_path_str}")

        # Schedule the temporary zip file for deletion after the response is sent
        background_tasks.add_task(cleanup_file, zip_file_path)

        # Send the zip file
        return FileResponse(
            path=zip_file_path,
            filename=zip_filename,
            media_type="application/zip",
            # The 'Content-Disposition' header is important to tell the browser it's an attachment
            headers={"Content-Disposition": f"attachment; filename={zip_filename}"}
        )

    except Exception as e:
        ui_logger.exception(f"Error while creating/processing 'Download All' zip: {e}")
        # Ensure we attempt to clean up a partially created zip file on error
        if zip_file_path.exists():
             cleanup_file(zip_file_path)
        return JSONResponse(status_code=500, content={"error": str(e)})

# --------------------------------------------------------------------------------
# 2. DOWNLOAD SINGLE FILE (Existing)
# --------------------------------------------------------------------------------
@router.get("/{file_path:path}")
async def download_file(file_path: str, request: Request):
    """
    Download file to client browser.
    Logs both successful and failed download attempts to the UI log.
    """
    base_dir = Path("backend/temp_uploads") # Define base dir
    full_path = base_dir / Path(file_path)
    client_ip = request.client.host if request.client else "unknown"

    ui_logger.info(f"UI download request (single) from {client_ip}: {file_path}")

    try:
        if full_path.exists() and full_path.is_file():
            ui_logger.info(f"File found: {full_path} — sending to client.")
            return FileResponse(full_path, filename=full_path.name, media_type="application/octet-stream")
        else:
            ui_logger.warning(f"File not found: {full_path}")
            return JSONResponse(status_code=404, content={"error": f"File not found: {file_path}"})
    except Exception as e:
        ui_logger.exception(f"Error while processing download for {file_path}: {e}")
        return JSONResponse(status_code=500, content={"error": str(e)})


