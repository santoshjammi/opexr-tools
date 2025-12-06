"""
Async comparison API endpoints with background job processing.
Uses DuckDB for fast queries on comparison results.
"""

from fastapi import APIRouter, HTTPException, BackgroundTasks, Query
from pydantic import BaseModel
from typing import Dict, Any, Optional
from pathlib import Path

from .compare_dask_duckdb import DaskDuckDBComparator
from .job_manager import JobManager, JobStatus


router = APIRouter(prefix="/compare/dask/async", tags=["async-comparison"])

# Initialize comparator and job manager
comparator = DaskDuckDBComparator()
job_manager = JobManager()


class StartComparisonRequest(BaseModel):
    """Request model for starting async comparison."""
    ecc_dataset: str
    ecp_dataset: str


@router.post("/start")
async def start_comparison(
    request: StartComparisonRequest,
    background_tasks: BackgroundTasks
):
    """
    Start async comparison in background.
    
    Returns job_id immediately for tracking progress.
    Comparison runs in background and streams results to DuckDB.
    
    Example:
        POST /compare/dask/async/start
        {
            "ecc_dataset": "ECCSEP01",
            "ecp_dataset": "ECP_1"
        }
    
    Returns:
        {
            "job_id": "uuid",
            "status": "pending",
            "message": "Comparison started"
        }
    """
    try:
        # Create job first
        job_id = job_manager.create_job(request.ecc_dataset, request.ecp_dataset)
        
        # Start comparison in background with the job_id
        def run_comparison():
            print(f"[DEBUG] Background task started for job {job_id}")
            try:
                print(f"[DEBUG] Starting comparison: {request.ecc_dataset} vs {request.ecp_dataset}")
                comparator.compare_datasets_async(
                    ecc_dataset=request.ecc_dataset,
                    ecp_dataset=request.ecp_dataset,
                    job_id=job_id
                )
                print(f"[DEBUG] Comparison completed for job {job_id}")
            except Exception as e:
                print(f"[ERROR] Background comparison error: {str(e)}")
                import traceback
                traceback.print_exc()
                # Update job with error
                job_manager.update_job(
                    job_id=job_id,
                    status=JobStatus.FAILED,
                    progress_message=f"Error: {str(e)}"
                )
        
        # Add comparison to background tasks
        background_tasks.add_task(run_comparison)
        
        return {
            "job_id": job_id,
            "status": "pending",
            "message": "Comparison started in background",
            "ecc_dataset": request.ecc_dataset,
            "ecp_dataset": request.ecp_dataset
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to start comparison: {str(e)}")


@router.get("/status/{job_id}")
async def get_job_status(job_id: str):
    """
    Get job status and progress.
    
    Example:
        GET /compare/dask/async/status/{job_id}
    
    Returns:
        {
            "job_id": "uuid",
            "status": "aggregating",
            "progress": 45.5,
            "progress_message": "Aggregating ECC data...",
            "created_at": "...",
            "updated_at": "...",
            "total_rows": 0,
            "processed_rows": 0,
            "summary": {...}  // Available when completed
        }
    """
    job = job_manager.get_job(job_id)
    
    if not job:
        raise HTTPException(status_code=404, detail=f"Job {job_id} not found")
    
    response = {
        "job_id": job["job_id"],
        "status": job["status"],
        "progress": job["progress"],
        "progress_message": job["progress_message"],
        "created_at": job["created_at"],
        "updated_at": job["updated_at"],
        "started_at": job.get("started_at"),
        "completed_at": job.get("completed_at"),
        "total_rows": job["total_rows"],
        "processed_rows": job["processed_rows"],
        "ecc_dataset": job["ecc_dataset"],
        "ecp_dataset": job["ecp_dataset"]
    }
    
    # Include summary if completed
    if job["status"] == JobStatus.COMPLETED:
        response["summary"] = job["metadata"].get("summary", {})
    
    # Include error if failed
    if job["status"] == JobStatus.FAILED:
        response["error"] = job.get("error")
    
    return response


@router.get("/results/{job_id}")
async def get_job_results(
    job_id: str,
    page: int = Query(1, ge=1, description="Page number (1-indexed)"),
    page_size: int = Query(100, ge=10, le=1000, description="Results per page"),
    sort_by: str = Query(
        "ecp_id,wage_category,ecc_amount DESC,ecp_amount DESC",
        description="Sort specification (comma-separated, use DESC for descending)"
    ),
    filter_status: Optional[str] = Query(None, regex="^(Matched|ECC Only|ECP Only)$", description="Filter by status")
):
    """
    Get comparison results with sorting and pagination.
    
    Results are queried from DuckDB for fast access.
    Multi-level sorting supported.
    
    Example:
        GET /compare/dask/async/results/{job_id}?page=1&page_size=100&sort_by=ecp_id,wage_category,ecc_amount DESC
    
    Sort Options:
        - ecp_id: Employee ID
        - wage_type: Wage type code
        - wage_category: Wage category (Earnings, Deductions, etc.)
        - ecc_amount: ECC amount
        - ecp_amount: ECP amount
        - difference: Difference amount
        - status: Match status
        
        Add DESC for descending order (default is ASC)
    
    Returns:
        {
            "status": "completed",
            "results": [...],
            "summary": {...},
            "pagination": {
                "page": 1,
                "page_size": 100,
                "total_rows": 2848213,
                "total_pages": 28483,
                "has_next": true,
                "has_prev": false
            }
        }
    """
    try:
        result = comparator.query_results(
            job_id=job_id,
            page=page,
            page_size=page_size,
            sort_by=sort_by,
            filter_status=filter_status
        )
        return result
        
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Query failed: {str(e)}")


@router.get("/jobs")
async def list_jobs(limit: int = Query(50, ge=1, le=200, description="Maximum jobs to return")):
    """
    List recent comparison jobs.
    
    Example:
        GET /compare/dask/async/jobs?limit=50
    
    Returns:
        {
            "jobs": [
                {
                    "job_id": "...",
                    "status": "completed",
                    "ecc_dataset": "ECCSEP01",
                    "ecp_dataset": "ECP_1",
                    "created_at": "...",
                    ...
                }
            ],
            "count": 50
        }
    """
    jobs = job_manager.list_jobs(limit=limit)
    
    return {
        "jobs": jobs,
        "count": len(jobs)
    }


@router.delete("/jobs/{job_id}")
async def delete_job(job_id: str):
    """
    Delete a job and its results.
    
    Example:
        DELETE /compare/dask/async/jobs/{job_id}
    
    Returns:
        {
            "message": "Job deleted successfully",
            "job_id": "..."
        }
    """
    success = job_manager.delete_job(job_id)
    
    if not success:
        raise HTTPException(status_code=404, detail=f"Job {job_id} not found")
    
    return {
        "message": "Job deleted successfully",
        "job_id": job_id
    }


@router.get("/summary/{job_id}")
async def get_job_summary(job_id: str):
    """
    Get summary statistics for a completed job.
    
    Example:
        GET /compare/dask/async/summary/{job_id}
    
    Returns:
        {
            "job_id": "...",
            "status": "completed",
            "summary": {
                "total_rows": 2848213,
                "total_ecc_amount": 14139376007803.07,
                "total_ecp_amount": 1393813571834.47,
                "matched_count": 36009,
                "ecc_only_count": 1648897,
                "ecp_only_count": 1163307
            },
            "employee_mapping_count": 99886,
            "wage_type_mapping_count": 383
        }
    """
    job = job_manager.get_job(job_id)
    
    if not job:
        raise HTTPException(status_code=404, detail=f"Job {job_id} not found")
    
    if job["status"] != JobStatus.COMPLETED:
        raise HTTPException(status_code=400, detail=f"Job not completed yet. Status: {job['status']}")
    
    return {
        "job_id": job_id,
        "status": job["status"],
        "summary": job["metadata"].get("summary", {}),
        "employee_mapping_count": job["metadata"].get("employee_mapping_count"),
        "wage_type_mapping_count": job["metadata"].get("wage_type_mapping_count"),
        "ecc_dataset": job["ecc_dataset"],
        "ecp_dataset": job["ecp_dataset"],
        "created_at": job["created_at"],
        "completed_at": job["completed_at"]
    }
