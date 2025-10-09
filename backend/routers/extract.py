from fastapi import APIRouter, UploadFile, File
from fastapi.responses import JSONResponse
import os
from services.runner import run_extraction
import uuid

router = APIRouter()

UPLOAD_DIR = "backend/temp_uploads"
os.makedirs(UPLOAD_DIR, exist_ok=True)

@router.post("/run")
async def upload_and_run(json_files: list[UploadFile] = File(...), tsv_file: UploadFile = File(...)):
    """
    Accept multiple JSON files and one TSV file.
    Runs the extraction logic and returns download links for results.
    """
    temp_id = str(uuid.uuid4())
    temp_folder = os.path.join(UPLOAD_DIR, temp_id)
    os.makedirs(temp_folder, exist_ok=True)

    # Save files
    saved_jsons = []
    for json_file in json_files:
        path = os.path.join(temp_folder, json_file.filename)
        with open(path, "wb") as f:
            f.write(await json_file.read())
        saved_jsons.append(path)

    tsv_path = os.path.join(temp_folder, tsv_file.filename)
    with open(tsv_path, "wb") as f:
        f.write(await tsv_file.read())

    # Run your existing main logic
    output_files = run_extraction(saved_jsons, tsv_path, temp_folder)

    return JSONResponse({"outputs": output_files})
