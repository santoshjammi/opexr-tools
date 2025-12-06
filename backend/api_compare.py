"""
API endpoints for dataset comparison
"""
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field
from typing import List, Optional, Dict, Any
from pathlib import Path

from backend.comparison import quick_compare, DatasetComparator

router = APIRouter(prefix="/compare", tags=["comparison"])


class ComparisonRequest(BaseModel):
    """Request model for comparison"""
    dataset_a: str = Field(..., description="Name of first dataset (without .json)")
    dataset_b: str = Field(..., description="Name of second dataset (without .json)")
    key_columns: List[str] = Field(..., description="Columns to use as unique identifiers")
    compare_columns: Optional[List[str]] = Field(None, description="Specific columns to compare (None = all)")
    tolerance: float = Field(0.01, description="Numeric tolerance for float comparisons")
    ignore_case: bool = Field(True, description="Ignore case in string comparisons")


class ComparisonSummary(BaseModel):
    """Summary statistics from comparison"""
    total_in_a: int
    total_in_b: int
    common_records: int
    only_in_a: int
    only_in_b: int
    identical_records: int
    records_with_differences: int
    match_rate: float


@router.post("/run")
async def run_comparison(request: ComparisonRequest) -> Dict[str, Any]:
    """
    Compare two datasets and return detailed results
    
    This endpoint compares two parsed datasets based on specified key columns
    and returns a comprehensive report including:
    - Summary statistics
    - Records with differences
    - Records only in A or B
    - Sample of identical records
    
    Example:
        ```json
        {
            "dataset_a": "ECCSEP05",
            "dataset_b": "ECP_7",
            "key_columns": ["Pers.No.", "For-period", "WT"],
            "tolerance": 0.01,
            "ignore_case": true
        }
        ```
    """
    try:
        result = quick_compare(
            dataset_a=request.dataset_a,
            dataset_b=request.dataset_b,
            key_columns=request.key_columns,
            compare_columns=request.compare_columns,
            tolerance=request.tolerance,
            ignore_case=request.ignore_case
        )
        
        return result
        
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Comparison error: {str(e)}")


@router.get("/history")
async def get_comparison_history() -> Dict[str, Any]:
    """Get list of previous comparison results"""
    results_dir = Path(__file__).parent.parent / 'data' / 'results'
    
    if not results_dir.exists():
        return {"comparisons": [], "count": 0}
    
    comparisons = []
    for file in results_dir.glob('comparison_*.json'):
        comparisons.append({
            'filename': file.name,
            'timestamp': file.stat().st_mtime,
            'size_bytes': file.stat().st_size
        })
    
    # Sort by timestamp descending
    comparisons.sort(key=lambda x: x['timestamp'], reverse=True)
    
    return {
        "comparisons": comparisons,
        "count": len(comparisons)
    }


@router.post("/export")
async def export_comparison(request: ComparisonRequest) -> Dict[str, Any]:
    """
    Run comparison and save results to file
    
    This endpoint runs the comparison and saves the full results to a JSON file
    in the data/results directory.
    """
    try:
        result = quick_compare(
            dataset_a=request.dataset_a,
            dataset_b=request.dataset_b,
            key_columns=request.key_columns,
            compare_columns=request.compare_columns,
            tolerance=request.tolerance,
            ignore_case=request.ignore_case
        )
        
        # Save to file
        results_dir = Path(__file__).parent.parent / 'data' / 'results'
        results_dir.mkdir(parents=True, exist_ok=True)
        
        filename = f"comparison_{result['comparison_id']}_{request.dataset_a}_vs_{request.dataset_b}.json"
        output_path = results_dir / filename
        
        comparator = DatasetComparator(Path("dummy"), Path("dummy"))  # Dummy paths, not used for export
        comparator.export_comparison(result, output_path)
        
        return {
            "message": "Comparison saved successfully",
            "filename": filename,
            "path": str(output_path),
            "summary": result['summary']
        }
        
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Export error: {str(e)}")


@router.get("/summary/{dataset_a}/{dataset_b}")
async def get_quick_summary(
    dataset_a: str,
    dataset_b: str,
    key_columns: str = "Pers.No.,For-period,WT"
) -> ComparisonSummary:
    """
    Get a quick summary comparison without full details
    
    Args:
        dataset_a: First dataset name
        dataset_b: Second dataset name  
        key_columns: Comma-separated list of key columns
    
    Returns:
        Summary statistics only (faster than full comparison)
    """
    try:
        key_list = [k.strip() for k in key_columns.split(',')]
        
        result = quick_compare(
            dataset_a=dataset_a,
            dataset_b=dataset_b,
            key_columns=key_list,
            compare_columns=[],  # Don't compare values, just count matches
            tolerance=0.01,
            ignore_case=True
        )
        
        return ComparisonSummary(**result['summary'])
        
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Summary error: {str(e)}")
