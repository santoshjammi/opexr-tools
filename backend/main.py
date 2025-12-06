"""
FastAPI application for TXT/CSV data parsing and conversion to JSON.
Supports large file processing with Dask for scalability.
"""
from fastapi import FastAPI, UploadFile, File, HTTPException, BackgroundTasks, Query
from fastapi.responses import FileResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
from typing import Optional, List, Dict, Any
import uuid
import os
from datetime import datetime
from pathlib import Path

from .parser import DaskTxtParser
from .models import ParseRequest, ParseResponse, JobStatus, JobStatusResponse
from .api_data import router as data_router
from .api_compare import router as compare_router
from .api_mapped_compare import router as mapped_compare_router
from .api_dask_compare import router as dask_compare_router
from .api_dask_async import router as dask_async_router

app = FastAPI(
    title="Data Comparison API",
    description="FastAPI service for parsing TXT/CSV files and converting to JSON using Dask",
    version="1.0.0"
)

# Include routers
app.include_router(data_router)
app.include_router(compare_router)
app.include_router(mapped_compare_router)
app.include_router(dask_compare_router)
app.include_router(dask_async_router)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Storage directories
DATA_DIR = Path("data")
UPLOADS_DIR = DATA_DIR / "uploads"
RESULTS_DIR = DATA_DIR / "results"
JOBS_DIR = DATA_DIR / "jobs"

for dir_path in [UPLOADS_DIR, RESULTS_DIR, JOBS_DIR]:
    dir_path.mkdir(parents=True, exist_ok=True)

# In-memory job tracker (use Redis in production)
job_tracker: Dict[str, Dict[str, Any]] = {}


@app.get("/")
async def root():
    """Root endpoint with API information."""
    return {
        "service": "Data Comparison API",
        "version": "1.0.0",
        "endpoints": {
            "parse": "/parse",
            "status": "/status/{job_id}",
            "result": "/result/{job_id}",
            "jobs": "/jobs"
        }
    }


@app.post("/parse", response_model=ParseResponse)
async def parse_txt_to_json(
    background_tasks: BackgroundTasks,
    file: UploadFile = File(...),
    delimiter: str = Query("\t", description="Delimiter character (default: tab)"),
    dataset_name: Optional[str] = Query(None, description="Dataset identifier"),
    encoding: str = Query("utf-8", description="File encoding"),
):
    """
    Upload a TXT/CSV file and convert it to JSON using Dask.
    
    - **file**: TXT or CSV file to parse
    - **delimiter**: Field delimiter (default: tab)
    - **dataset_name**: Optional dataset identifier
    - **encoding**: File encoding (default: utf-8)
    
    Returns job_id for tracking the async parsing job.
    """
    # Generate job ID
    job_id = str(uuid.uuid4())
    timestamp = datetime.now().strftime("%Y-%m-%d_%H%M%S")
    
    # Determine dataset name
    if not dataset_name:
        dataset_name = Path(file.filename).stem
    
    # Create job directory
    job_dir = UPLOADS_DIR / dataset_name / f"{timestamp}_{job_id}"
    job_dir.mkdir(parents=True, exist_ok=True)
    
    # Save uploaded file
    file_path = job_dir / file.filename
    try:
        content = await file.read()
        file_path.write_bytes(content)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to save file: {str(e)}")
    
    # Initialize job tracker
    job_tracker[job_id] = {
        "job_id": job_id,
        "status": JobStatus.QUEUED,
        "dataset_name": dataset_name,
        "file_path": str(file_path),
        "file_name": file.filename,
        "delimiter": delimiter,
        "encoding": encoding,
        "created_at": datetime.now().isoformat(),
        "updated_at": datetime.now().isoformat(),
        "result_path": None,
        "error": None,
        "rows_processed": 0,
    }
    
    # Schedule background parsing task
    background_tasks.add_task(
        parse_file_task,
        job_id=job_id,
        file_path=file_path,
        delimiter=delimiter,
        encoding=encoding,
        dataset_name=dataset_name
    )
    
    return ParseResponse(
        job_id=job_id,
        status=JobStatus.QUEUED,
        dataset_name=dataset_name,
        message=f"File uploaded successfully. Processing started."
    )


async def parse_file_task(
    job_id: str,
    file_path: Path,
    delimiter: str,
    encoding: str,
    dataset_name: str
):
    """Background task to parse TXT file to JSON using Dask."""
    try:
        # Update status to running
        job_tracker[job_id]["status"] = JobStatus.RUNNING
        job_tracker[job_id]["updated_at"] = datetime.now().isoformat()
        
        # Initialize parser
        parser = DaskTxtParser(
            file_path=str(file_path),
            delimiter=delimiter,
            encoding=encoding
        )
        
        # Parse file
        result = parser.parse_to_json()
        
        # Save result
        result_dir = RESULTS_DIR / job_id
        result_dir.mkdir(parents=True, exist_ok=True)
        result_path = result_dir / f"{dataset_name}.json"
        
        parser.save_json(result, str(result_path))
        
        # Update job tracker
        job_tracker[job_id].update({
            "status": JobStatus.COMPLETED,
            "result_path": str(result_path),
            "rows_processed": result.get("metadata", {}).get("rows", 0),
            "updated_at": datetime.now().isoformat()
        })
        
    except Exception as e:
        job_tracker[job_id].update({
            "status": JobStatus.FAILED,
            "error": str(e),
            "updated_at": datetime.now().isoformat()
        })


@app.get("/status/{job_id}", response_model=JobStatusResponse)
async def get_job_status(job_id: str):
    """
    Get the status of a parsing job.
    
    - **job_id**: Job identifier returned from /parse endpoint
    """
    if job_id not in job_tracker:
        raise HTTPException(status_code=404, detail=f"Job {job_id} not found")
    
    job = job_tracker[job_id]
    return JobStatusResponse(**job)


@app.get("/result/{job_id}")
async def get_result(job_id: str):
    """
    Download the JSON result of a completed parsing job.
    
    - **job_id**: Job identifier
    """
    if job_id not in job_tracker:
        raise HTTPException(status_code=404, detail=f"Job {job_id} not found")
    
    job = job_tracker[job_id]
    
    if job["status"] != JobStatus.COMPLETED:
        raise HTTPException(
            status_code=400,
            detail=f"Job is not completed. Current status: {job['status']}"
        )
    
    result_path = job["result_path"]
    if not result_path or not Path(result_path).exists():
        raise HTTPException(status_code=404, detail="Result file not found")
    
    return FileResponse(
        path=result_path,
        media_type="application/json",
        filename=f"{job['dataset_name']}.json"
    )


@app.get("/jobs")
async def list_jobs(
    dataset_name: Optional[str] = Query(None, description="Filter by dataset name"),
    status: Optional[JobStatus] = Query(None, description="Filter by status"),
    limit: int = Query(10, ge=1, le=100, description="Number of jobs to return")
):
    """
    List all parsing jobs with optional filters.
    
    - **dataset_name**: Filter by dataset name
    - **status**: Filter by job status
    - **limit**: Maximum number of jobs to return
    """
    jobs = list(job_tracker.values())
    
    # Apply filters
    if dataset_name:
        jobs = [j for j in jobs if j["dataset_name"] == dataset_name]
    if status:
        jobs = [j for j in jobs if j["status"] == status]
    
    # Sort by created_at descending
    jobs.sort(key=lambda x: x["created_at"], reverse=True)
    
    # Limit results
    jobs = jobs[:limit]
    
    return {
        "total": len(jobs),
        "jobs": jobs
    }


@app.delete("/job/{job_id}")
async def delete_job(job_id: str):
    """
    Delete a job and its associated files.
    
    - **job_id**: Job identifier
    """
    if job_id not in job_tracker:
        raise HTTPException(status_code=404, detail=f"Job {job_id} not found")
    
    job = job_tracker[job_id]
    
    # Delete result file if exists
    if job["result_path"]:
        result_path = Path(job["result_path"])
        if result_path.exists():
            result_path.unlink()
        # Remove result directory if empty
        if result_path.parent.exists() and not list(result_path.parent.iterdir()):
            result_path.parent.rmdir()
    
    # Delete uploaded file if exists
    if job["file_path"]:
        file_path = Path(job["file_path"])
        if file_path.exists():
            file_path.unlink()
    
    # Remove from tracker
    del job_tracker[job_id]
    
    return {"message": f"Job {job_id} deleted successfully"}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
