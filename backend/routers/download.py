from fastapi import APIRouter, Request
from fastapi.responses import FileResponse, JSONResponse
from pathlib import Path
from helpers.logger_config import setup_logger

router = APIRouter()

# Create a dedicated UI logger (goes to app_ui_YYYY-MM-DD.log)
ui_logger = setup_logger("ui_download", ui=True)

@router.get("/{file_path:path}")
async def download_file(file_path: str, request: Request):
    """
    Download file to client browser.
    Logs both successful and failed download attempts to the UI log.
    """
    full_path = Path("backend/temp_uploads") / Path(file_path)
    client_ip = request.client.host if request.client else "unknown"

    ui_logger.info(f"UI download request from {client_ip}: {file_path}")

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
