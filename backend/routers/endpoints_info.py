from fastapi import APIRouter
from main import SOURCE_EXTRACTORS, TARGET_EXTRACTORS

router = APIRouter()

@router.get("/info/supported")
async def get_supported_sources_targets():
    """
    Returns supported sources and targets with full component names.
    """
    sources = sorted(list(SOURCE_EXTRACTORS.keys()))
    targets = sorted(list(TARGET_EXTRACTORS.keys()))
    return {"sources": sources, "targets": targets}
