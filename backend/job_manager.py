"""
Job management system for async comparison operations.
Tracks job status, progress, and results storage.
"""

from pathlib import Path
from typing import Dict, Optional, Any
from datetime import datetime
from enum import Enum
import json
import uuid


class JobStatus(str, Enum):
    """Job status states."""
    PENDING = "pending"
    LOADING_DATA = "loading_data"
    AGGREGATING = "aggregating"
    MAPPING = "mapping"
    MERGING = "merging"
    STORING = "storing"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


class JobManager:
    """Manages comparison job lifecycle and status."""
    
    def __init__(self, jobs_dir: Path = None):
        """
        Initialize job manager.
        
        Args:
            jobs_dir: Directory to store job metadata and results
        """
        if jobs_dir is None:
            jobs_dir = Path(__file__).parent.parent / "data" / "jobs"
        
        self.jobs_dir = Path(jobs_dir)
        self.jobs_dir.mkdir(parents=True, exist_ok=True)
        
        # In-memory cache for quick access
        self._jobs_cache: Dict[str, Dict[str, Any]] = {}
    
    def create_job(
        self,
        ecc_dataset: str,
        ecp_dataset: str,
        job_type: str = "dask_comparison"
    ) -> str:
        """
        Create a new job.
        
        Args:
            ecc_dataset: ECC dataset name
            ecp_dataset: ECP dataset name
            job_type: Type of job
            
        Returns:
            job_id: Unique job identifier
        """
        job_id = str(uuid.uuid4())
        
        job_data = {
            "job_id": job_id,
            "job_type": job_type,
            "status": JobStatus.PENDING,
            "ecc_dataset": ecc_dataset,
            "ecp_dataset": ecp_dataset,
            "created_at": datetime.now().isoformat(),
            "updated_at": datetime.now().isoformat(),
            "started_at": None,
            "completed_at": None,
            "progress": 0.0,
            "progress_message": "Job created, waiting to start...",
            "total_rows": 0,
            "processed_rows": 0,
            "result_path": None,
            "error": None,
            "metadata": {}
        }
        
        # Save to disk and cache
        self._save_job(job_id, job_data)
        self._jobs_cache[job_id] = job_data
        
        return job_id
    
    def get_job(self, job_id: str) -> Optional[Dict[str, Any]]:
        """
        Get job status and metadata.
        
        Args:
            job_id: Job identifier
            
        Returns:
            Job data dictionary or None if not found
        """
        # Check cache first
        if job_id in self._jobs_cache:
            return self._jobs_cache[job_id]
        
        # Load from disk
        job_file = self.jobs_dir / f"{job_id}.json"
        if job_file.exists():
            with open(job_file, 'r') as f:
                job_data = json.load(f)
                self._jobs_cache[job_id] = job_data
                return job_data
        
        return None
    
    def update_job(
        self,
        job_id: str,
        status: Optional[JobStatus] = None,
        progress: Optional[float] = None,
        progress_message: Optional[str] = None,
        processed_rows: Optional[int] = None,
        total_rows: Optional[int] = None,
        result_path: Optional[str] = None,
        error: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None
    ):
        """
        Update job status and progress.
        
        Args:
            job_id: Job identifier
            status: New job status
            progress: Progress percentage (0-100)
            progress_message: Human-readable progress message
            processed_rows: Number of rows processed
            total_rows: Total rows to process
            result_path: Path to result file
            error: Error message if failed
            metadata: Additional metadata
        """
        job_data = self.get_job(job_id)
        if not job_data:
            raise ValueError(f"Job {job_id} not found")
        
        # Update fields
        if status is not None:
            job_data["status"] = status
            if status == JobStatus.LOADING_DATA and job_data["started_at"] is None:
                job_data["started_at"] = datetime.now().isoformat()
            elif status in (JobStatus.COMPLETED, JobStatus.FAILED):
                job_data["completed_at"] = datetime.now().isoformat()
        
        if progress is not None:
            job_data["progress"] = min(100.0, max(0.0, progress))
        
        if progress_message is not None:
            job_data["progress_message"] = progress_message
        
        if processed_rows is not None:
            job_data["processed_rows"] = processed_rows
        
        if total_rows is not None:
            job_data["total_rows"] = total_rows
        
        if result_path is not None:
            job_data["result_path"] = result_path
        
        if error is not None:
            job_data["error"] = error
            job_data["status"] = JobStatus.FAILED
        
        if metadata is not None:
            job_data["metadata"].update(metadata)
        
        job_data["updated_at"] = datetime.now().isoformat()
        
        # Save to disk and cache
        self._save_job(job_id, job_data)
        self._jobs_cache[job_id] = job_data
    
    def list_jobs(self, limit: int = 50) -> list[Dict[str, Any]]:
        """
        List recent jobs.
        
        Args:
            limit: Maximum number of jobs to return
            
        Returns:
            List of job data dictionaries
        """
        job_files = sorted(
            self.jobs_dir.glob("*.json"),
            key=lambda p: p.stat().st_mtime,
            reverse=True
        )
        
        jobs = []
        for job_file in job_files[:limit]:
            try:
                with open(job_file, 'r') as f:
                    jobs.append(json.load(f))
            except:
                pass
        
        return jobs
    
    def delete_job(self, job_id: str) -> bool:
        """
        Delete a job and its results.
        
        Args:
            job_id: Job identifier
            
        Returns:
            True if deleted, False if not found
        """
        job_file = self.jobs_dir / f"{job_id}.json"
        
        # Delete result DuckDB file if exists
        job_data = self.get_job(job_id)
        if job_data and job_data.get("result_path"):
            result_path = Path(job_data["result_path"])
            if result_path.exists():
                result_path.unlink()
        
        # Delete job metadata
        if job_file.exists():
            job_file.unlink()
            if job_id in self._jobs_cache:
                del self._jobs_cache[job_id]
            return True
        
        return False
    
    def _save_job(self, job_id: str, job_data: Dict[str, Any]):
        """Save job data to disk."""
        job_file = self.jobs_dir / f"{job_id}.json"
        with open(job_file, 'w') as f:
            json.dump(job_data, f, indent=2)
    
    def get_result_db_path(self, job_id: str) -> Path:
        """
        Get the path to the DuckDB result file for a job.
        
        Args:
            job_id: Job identifier
            
        Returns:
            Path to DuckDB file
        """
        results_dir = self.jobs_dir.parent / "results"
        results_dir.mkdir(parents=True, exist_ok=True)
        return results_dir / f"{job_id}.duckdb"
