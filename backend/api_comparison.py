"""
FastAPI router for serving comparison viewer data.
Provides endpoints for employee list, search, and individual employee data.
"""

from fastapi import APIRouter, HTTPException, Query, UploadFile, File
from pathlib import Path
import json
from typing import Optional

router = APIRouter(prefix="/api/comparison", tags=["comparison"])

# Configuration
JSON_DIR = Path('data/json')


@router.get("/employees")
async def get_employees(
    page: int = Query(1, ge=1),
    per_page: int = Query(100, ge=1, le=500)
):
    """Get list of all employees with pagination."""
    try:
        # Load index
        index_file = JSON_DIR / 'index.json'
        with open(index_file, 'r', encoding='utf-8') as f:
            employees = json.load(f)
        
        # Pagination
        start = (page - 1) * per_page
        end = start + per_page
        
        return {
            'total': len(employees),
            'page': page,
            'per_page': per_page,
            'total_pages': (len(employees) + per_page - 1) // per_page,
            'employees': employees[start:end]
        }
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/search")
async def search_employees(
    q: str = Query(..., min_length=1),
    limit: int = Query(50, ge=1, le=100)
):
    """Search employees by name or PERNR."""
    try:
        query = q.strip().lower()
        
        # Load index
        index_file = JSON_DIR / 'index.json'
        with open(index_file, 'r', encoding='utf-8') as f:
            employees = json.load(f)
        
        # Search by name, ECC PERNR, or ECP PERNR
        results = []
        for emp in employees:
            if (query in emp['name'].lower() or 
                query in emp['ecc_pernr'].lower() or 
                query in emp['ecp_pernr'].lower()):
                results.append(emp)
        
        return {
            'query': query,
            'total': len(results),
            'results': results[:limit]
        }
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/employee/{pernr}")
async def get_employee(pernr: str):
    """Get detailed data for a specific employee."""
    try:
        json_file = JSON_DIR / f'{pernr}.json'
        
        if not json_file.exists():
            raise HTTPException(status_code=404, detail='Employee not found')
        
        with open(json_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        return data
    
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/upload")
async def upload_comparison(file: UploadFile = File(...)):
    """Upload a comparison file and convert to JSON."""
    try:
        if not file.filename.endswith('.txt'):
            raise HTTPException(status_code=400, detail='Only .txt files are accepted')
        
        # Save the uploaded file
        upload_dir = Path('data/uploads')
        upload_dir.mkdir(parents=True, exist_ok=True)
        
        file_path = upload_dir / file.filename
        
        contents = await file.read()
        with open(file_path, 'wb') as f:
            f.write(contents)
        
        # TODO: Convert to JSON (call the conversion logic here)
        
        return {
            'success': True,
            'filename': file.filename,
            'message': 'File uploaded successfully'
        }
    
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/stats")
async def get_stats():
    """Get statistics about the dataset."""
    try:
        index_file = JSON_DIR / 'index.json'
        with open(index_file, 'r', encoding='utf-8') as f:
            employees = json.load(f)
        
        total_employees = len(employees)
        total_entries = sum(emp['total_entries'] for emp in employees)
        
        return {
            'total_employees': total_employees,
            'total_entries': total_entries,
            'avg_entries_per_employee': total_entries / total_employees if total_employees > 0 else 0
        }
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
