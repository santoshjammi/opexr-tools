"""
FastAPI endpoints for loading and querying JSON data.
"""
from fastapi import APIRouter, HTTPException, Query
from typing import Optional, List, Dict, Any
from pathlib import Path
import json

router = APIRouter(prefix="/data", tags=["data"])

# Data cache
_data_cache: Dict[str, Dict[str, Any]] = {}


def load_json_file(dataset_name: str) -> Dict[str, Any]:
    """Load JSON file and cache it."""
    # Use absolute path relative to this file's location
    encoded_dir = Path(__file__).parent.parent / "data" / "encoded"
    file_path = encoded_dir / f"{dataset_name}.json"
    
    if not file_path.exists():
        raise FileNotFoundError(f"File not found: {file_path}")
    
    # Check cache first
    cache_key = str(file_path.absolute())
    if cache_key in _data_cache:
        return _data_cache[cache_key]
    
    # Load from file
    with open(file_path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    # Cache it
    _data_cache[cache_key] = data
    return data


@router.get("/metadata/{dataset_name}")
async def get_metadata(dataset_name: str):
    """
    Get metadata for a dataset.
    
    - **dataset_name**: Name of the dataset (e.g., 'ECCSEP05')
    """
    try:
        data = load_json_file(dataset_name)
        
        return {
            "dataset": dataset_name,
            "metadata": data.get("metadata", {}),
            "cache_key": str((Path(__file__).parent.parent / "data" / "encoded" / f"{dataset_name}.json").absolute())
        }
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error loading metadata: {str(e)}")


@router.get("/records/{dataset_name}")
async def get_records(
    dataset_name: str,
    skip: int = Query(0, ge=0, description="Number of records to skip"),
    limit: int = Query(100, ge=1, le=1000, description="Maximum records to return"),
    pers_no: Optional[int] = Query(None, description="Filter by Personnel Number"),
    emp_id: Optional[str] = Query(None, description="Filter by Employee ID (encoded name)"),
):
    """
    Get records from a dataset with pagination and filtering.
    
    - **dataset_name**: Name of the dataset (e.g., 'ECCSEP05')
    - **skip**: Number of records to skip (for pagination)
    - **limit**: Maximum number of records to return
    - **pers_no**: Filter by Personnel Number
    - **emp_id**: Filter by Employee ID (encoded name)
    """
    try:
        data = load_json_file(dataset_name)
        
        records = data.get("data", [])
        
        # Apply filters
        if pers_no is not None:
            records = [r for r in records if r.get("Pers.No.") == pers_no]
        
        if emp_id is not None:
            records = [r for r in records if r.get("Last name First name") == emp_id]
        
        # Pagination
        total = len(records)
        paginated = records[skip:skip + limit]
        
        return {
            "dataset": dataset_name,
            "total": total,
            "skip": skip,
            "limit": limit,
            "count": len(paginated),
            "records": paginated
        }
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error loading records: {str(e)}")


@router.get("/employee/{dataset_name}/{emp_id}")
async def get_employee_records(
    dataset_name: str,
    emp_id: str
):
    """
    Get all records for a specific employee.
    
    - **dataset_name**: Name of the dataset (e.g., 'ECCSEP05')
    - **emp_id**: Employee ID (e.g., 'EMP_00001')
    """
    try:
        data = load_json_file(dataset_name)
        
        records = data.get("data", [])
        employee_records = [r for r in records if r.get("Last name First name") == emp_id]
        
        if not employee_records:
            raise HTTPException(
                status_code=404,
                detail=f"No records found for employee {emp_id}"
            )
        
        # Calculate summary
        total_amount = sum(r.get("Amount", 0) or 0 for r in employee_records)
        wage_types = list(set(r.get("WT") for r in employee_records if r.get("WT")))
        
        return {
            "dataset": dataset_name,
            "employee_id": emp_id,
            "total_records": len(employee_records),
            "total_amount": total_amount,
            "wage_types_count": len(wage_types),
            "wage_types": wage_types[:10],  # Limit to first 10
            "records": employee_records
        }
    except HTTPException:
        raise
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error loading employee records: {str(e)}")


@router.get("/search/{dataset_name}")
async def search_records(
    dataset_name: str,
    wage_type: Optional[str] = Query(None, description="Filter by Wage Type (WT)"),
    period: Optional[int] = Query(None, description="Filter by For-period"),
    min_amount: Optional[float] = Query(None, description="Minimum amount"),
    max_amount: Optional[float] = Query(None, description="Maximum amount"),
    limit: int = Query(100, ge=1, le=1000)
):
    """
    Search and filter records by various criteria.
    
    - **dataset_name**: Name of the dataset
    - **wage_type**: Filter by Wage Type
    - **period**: Filter by For-period
    - **min_amount**: Minimum amount filter
    - **max_amount**: Maximum amount filter
    - **limit**: Maximum number of results
    """
    try:
        data = load_json_file(dataset_name)
        
        records = data.get("data", [])
        
        # Apply filters
        if wage_type:
            records = [r for r in records if r.get("WT") == wage_type]
        
        if period is not None:
            records = [r for r in records if r.get("For-period") == period]
        
        if min_amount is not None:
            records = [r for r in records if (r.get("Amount") or 0) >= min_amount]
        
        if max_amount is not None:
            records = [r for r in records if (r.get("Amount") or 0) <= max_amount]
        
        # Limit results
        results = records[:limit]
        
        return {
            "dataset": dataset_name,
            "filters": {
                "wage_type": wage_type,
                "period": period,
                "min_amount": min_amount,
                "max_amount": max_amount
            },
            "total_matches": len(records),
            "returned": len(results),
            "records": results
        }
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error searching records: {str(e)}")


@router.get("/stats/{dataset_name}")
async def get_statistics(dataset_name: str):
    """
    Get statistical summary of the dataset.
    
    - **dataset_name**: Name of the dataset (e.g., 'ECCSEP05')
    """
    import math
    
    def safe_num(value, default=0):
        """Convert value to float, handling NaN and None."""
        if value is None:
            return default
        if isinstance(value, (int, float)):
            return default if math.isnan(value) or math.isinf(value) else value
        return default
    
    try:
        data = load_json_file(dataset_name)
        
        metadata = data.get("metadata", {})
        records = data.get("data", [])
        
        # Calculate statistics
        unique_employees = len(set(r.get("Last name First name") for r in records if r.get("Last name First name")))
        unique_wage_types = len(set(r.get("WT") for r in records if r.get("WT")))
        
        # Handle amounts with NaN safety
        amounts = [safe_num(r.get("Amount")) for r in records]
        total_amount = sum(amounts)
        avg_amount = total_amount / len(records) if records else 0
        amounts.sort()
        
        return {
            "dataset": dataset_name,
            "file_info": {
                "source_file": metadata.get("source_file"),
                "rows": metadata.get("rows"),
                "file_size_bytes": metadata.get("file_size_bytes"),
                "parsed_at": metadata.get("parsed_at")
            },
            "statistics": {
                "unique_employees": unique_employees,
                "unique_wage_types": unique_wage_types,
                "total_records": len(records),
                "total_amount": round(total_amount, 2) if not math.isnan(total_amount) else 0,
                "average_amount": round(avg_amount, 2) if not math.isnan(avg_amount) else 0,
                "min_amount": min(amounts) if amounts else 0,
                "max_amount": max(amounts) if amounts else 0,
                "median_amount": amounts[len(amounts)//2] if amounts else 0
            }
        }
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error calculating statistics: {str(e)}")


@router.delete("/cache")
async def clear_cache():
    """Clear the data cache to free memory."""
    global _data_cache
    count = len(_data_cache)
    _data_cache.clear()
    return {
        "message": f"Cache cleared successfully",
        "items_cleared": count
    }


@router.get("/available")
async def list_available_datasets():
    """List all available datasets in the encoded directory."""
    try:
        # Use absolute path relative to this file's location
        encoded_dir = Path(__file__).parent.parent / "data" / "encoded"
        if not encoded_dir.exists():
            return {"datasets": [], "count": 0}
        
        json_files = list(encoded_dir.glob("*.json"))
        datasets = [f.stem for f in json_files]
        
        return {
            "datasets": datasets,
            "count": len(datasets),
            "directory": str(encoded_dir.absolute())
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error listing datasets: {str(e)}")


@router.delete("/datasets/{dataset_name}")
async def delete_dataset(dataset_name: str):
    """
    Delete a specific dataset.
    
    - **dataset_name**: Name of the dataset to delete (e.g., 'ECCSEP05')
    """
    try:
        encoded_dir = Path(__file__).parent.parent / "data" / "encoded"
        file_path = encoded_dir / f"{dataset_name}.json"
        
        if not file_path.exists():
            raise HTTPException(status_code=404, detail=f"Dataset '{dataset_name}' not found")
        
        # Remove from cache if present
        cache_key = str(file_path.absolute())
        if cache_key in _data_cache:
            del _data_cache[cache_key]
        
        # Delete the file
        file_path.unlink()
        
        return {
            "message": f"Dataset '{dataset_name}' deleted successfully",
            "deleted_file": str(file_path)
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error deleting dataset: {str(e)}")


@router.delete("/datasets")
async def delete_all_datasets():
    """
    Delete ALL datasets in the encoded directory.
    
    WARNING: This will permanently delete all JSON files in the encoded directory.
    """
    try:
        encoded_dir = Path(__file__).parent.parent / "data" / "encoded"
        if not encoded_dir.exists():
            return {
                "message": "No datasets directory found",
                "deleted_count": 0
            }
        
        json_files = list(encoded_dir.glob("*.json"))
        deleted_count = 0
        deleted_files = []
        
        for file_path in json_files:
            # Remove from cache if present
            cache_key = str(file_path.absolute())
            if cache_key in _data_cache:
                del _data_cache[cache_key]
            
            # Delete the file
            file_path.unlink()
            deleted_files.append(file_path.stem)
            deleted_count += 1
        
        return {
            "message": f"Successfully deleted {deleted_count} dataset(s)",
            "deleted_count": deleted_count,
            "deleted_datasets": deleted_files,
            "directory": str(encoded_dir.absolute())
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error deleting datasets: {str(e)}")
