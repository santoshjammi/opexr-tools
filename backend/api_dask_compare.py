"""
Dask-based comparison API endpoints with pagination support.
Optimized for large-scale dataset comparison.
"""

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel
from typing import Dict, Any, Optional
from pathlib import Path

from .compare_dask import DaskDataComparator


router = APIRouter(prefix="/compare/dask", tags=["dask-comparison"])

# Initialize Dask comparator (reuse cluster across requests)
dask_comparator = DaskDataComparator()


class ComparisonRequest(BaseModel):
    """Request model for dataset comparison."""
    ecc_dataset: str
    ecp_dataset: str
    page: int = 1
    page_size: int = 100


@router.get("/datasets/{ecc_dataset}/{ecp_dataset}")
async def compare_datasets_dask(
    ecc_dataset: str,
    ecp_dataset: str,
    page: int = Query(1, ge=1, description="Page number (1-indexed)"),
    page_size: int = Query(100, ge=10, le=1000, description="Results per page")
):
    """
    Dask-based comparison for large datasets with pagination.
    
    Uses parallel aggregation strategy:
    - Groups by employee (mapped via PERNR_ECC_ECP.xlsx)
    - Aggregates amounts by wage type
    - Performs outer join to find matches and differences
    - Returns paginated results sorted by largest differences
    
    Example: GET /compare/dask/datasets/ECC_MASTER/ECP_MASTER?page=1&page_size=100
    
    Returns:
    - summary: Aggregated statistics (totals, counts)
    - results: Paginated comparison results
    - pagination: Page info (current page, total pages, has_next/prev)
    - mappings: Mapping information
    """
    try:
        result = dask_comparator.compare_datasets_aggregated(
            ecc_dataset=ecc_dataset,
            ecp_dataset=ecp_dataset,
            page=page,
            page_size=page_size
        )
        return result
        
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Dask comparison error: {str(e)}")


@router.get("/status")
async def get_dask_status():
    """
    Get Dask cluster status and dashboard link.
    
    Returns information about the running Dask cluster.
    """
    try:
        client = dask_comparator.start_cluster()
        
        return {
            'status': 'running',
            'dashboard': client.dashboard_link,
            'n_workers': len(client.scheduler_info()['workers']),
            'workers': list(client.scheduler_info()['workers'].keys())
        }
    except Exception as e:
        return {
            'status': 'error',
            'message': str(e)
        }


@router.post("/cluster/start")
async def start_cluster():
    """Start the Dask cluster for processing."""
    try:
        client = dask_comparator.start_cluster()
        return {
            'status': 'started',
            'dashboard': client.dashboard_link,
            'n_workers': dask_comparator.n_workers
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to start cluster: {str(e)}")


@router.post("/cluster/stop")
async def stop_cluster():
    """Stop the Dask cluster."""
    try:
        dask_comparator.stop_cluster()
        return {'status': 'stopped'}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to stop cluster: {str(e)}")


@router.get("/export/{ecc_dataset}/{ecp_dataset}")
async def export_full_comparison(
    ecc_dataset: str,
    ecp_dataset: str,
    format: str = Query('parquet', regex='^(parquet|json)$', description="Export format")
):
    """
    Export full comparison results to Parquet or JSON.
    
    Use this for downloading complete results without pagination.
    Parquet format is recommended for large datasets.
    
    Example: GET /compare/dask/export/ECC_MASTER/ECP_MASTER?format=parquet
    
    Returns:
    - Path to exported file
    - Summary statistics
    """
    try:
        output_path = dask_comparator.export_full_comparison(
            ecc_dataset=ecc_dataset,
            ecp_dataset=ecp_dataset,
            output_format=format
        )
        
        return {
            'status': 'success',
            'output_path': str(output_path),
            'format': format,
            'message': f'Full comparison exported to {output_path}'
        }
        
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Export error: {str(e)}")


@router.get("/mappings")
async def get_dask_mappings_info():
    """
    Get information about loaded mappings.
    
    Returns employee and wage type mapping counts and samples.
    """
    try:
        emp_df, wt_df = dask_comparator.load_mappings()
        
        emp_map = emp_df.set_index('ECC PERNR')['ECP PERNR'].to_dict()
        wt_map = wt_df.set_index('WT')['Categories'].to_dict()
        
        return {
            'employee_mappings': {
                'count': len(emp_map),
                'description': 'ECC PERNR to ECP PERNR mapping',
                'sample': dict(list(emp_map.items())[:10])
            },
            'wage_type_mappings': {
                'count': len(wt_map),
                'categories': list(set(wt_map.values())),
                'category_count': len(set(wt_map.values())),
                'description': 'Wage Type to Category classification',
                'sample': dict(list(wt_map.items())[:10])
            }
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error loading mappings: {str(e)}")
