from fastapi import APIRouter, Request
from main import SOURCE_EXTRACTORS, TARGET_EXTRACTORS
from helpers.logger_config import setup_logger

router = APIRouter()

# Dedicated loggers
backend_logger = setup_logger(__name__)
ui_logger = setup_logger("ui_info", ui=True)


@router.get("/info/supported")
async def get_supported_sources_targets(request: Request):
    """
    Returns supported sources and targets with full component names.
    Logs both UI and backend access.
    """
    client_ip = request.client.host if request.client else "unknown"
    ui_logger.info(f"UI requested supported sources and targets from {client_ip}")
    backend_logger.info(f"Processing /info/supported request for {client_ip}")

    try:
        sources = sorted(list(SOURCE_EXTRACTORS.keys()))
        targets = sorted(list(TARGET_EXTRACTORS.keys()))

        ui_logger.info(
            f"Returned {len(sources)} sources and {len(targets)} targets to {client_ip}"
        )
        backend_logger.debug(
            f"Supported sources: {sources}, targets: {targets}"
        )

        return {"sources": sources, "targets": targets}

    except Exception as e:
        ui_logger.exception(f"Error serving /info/supported to {client_ip}: {e}")
        backend_logger.exception(f"Failed to process /info/supported for {client_ip}: {e}")
        return {"error": str(e)}
