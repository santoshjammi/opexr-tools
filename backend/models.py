"""
Pydantic models for request/response validation.
"""
from pydantic import BaseModel, Field
from typing import Optional, List, Dict, Any
from enum import Enum
from datetime import datetime


class JobStatus(str, Enum):
    """Job status enumeration."""
    QUEUED = "queued"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"


class ParseRequest(BaseModel):
    """Request model for parsing endpoint."""
    delimiter: str = Field(default="\t", description="Field delimiter")
    dataset_name: Optional[str] = Field(None, description="Dataset identifier")
    encoding: str = Field(default="utf-8", description="File encoding")


class ParseResponse(BaseModel):
    """Response model for parse endpoint."""
    job_id: str = Field(..., description="Unique job identifier")
    status: JobStatus = Field(..., description="Job status")
    dataset_name: str = Field(..., description="Dataset name")
    message: str = Field(..., description="Status message")


class JobStatusResponse(BaseModel):
    """Response model for job status."""
    job_id: str
    status: JobStatus
    dataset_name: str
    file_path: str
    file_name: str
    delimiter: str
    encoding: str
    created_at: str
    updated_at: str
    result_path: Optional[str] = None
    error: Optional[str] = None
    rows_processed: int = 0


class DataRecord(BaseModel):
    """Generic data record model."""
    data: Dict[str, Any] = Field(..., description="Parsed data fields")


class JSONMetadata(BaseModel):
    """Metadata for JSON output."""
    source_file: str
    delimiter: str
    encoding: str
    rows: int
    columns: List[str]
    parsed_at: str
    job_id: Optional[str] = None
