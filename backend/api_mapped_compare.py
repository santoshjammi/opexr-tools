"""
Enhanced API endpoints for mapped data comparison.
Uses employee and wage type mappings for detailed comparison.
"""

from fastapi import APIRouter, HTTPException, UploadFile, File
from pydantic import BaseModel
from typing import Dict, Any, Optional
from pathlib import Path
import json
import uuid

from .compare import DataComparator


router = APIRouter(prefix="/compare/mapped", tags=["enhanced-comparison"])

# Initialize comparator with mappings
comparator = DataComparator()


@router.post("/upload")
async def compare_uploaded_files(
    ecc_file: UploadFile = File(..., description="ECC JSON file"),
    ecp_file: UploadFile = File(..., description="ECP JSON file")
):
    """
    Enhanced comparison using uploaded ECC and ECP JSON files.
    
    Uses PERNR_ECC_ECP.xlsx and wagetype_classification.xlsx mappings 
    to match employees and categorize wage types.
    
    Returns detailed comparison with amounts and differences.
    """
    try:
        # Save uploaded files temporarily
        temp_dir = Path(__file__).parent.parent / 'data' / 'temp'
        temp_dir.mkdir(parents=True, exist_ok=True)
        
        ecc_path = temp_dir / f"ecc_{uuid.uuid4()}.json"
        ecp_path = temp_dir / f"ecp_{uuid.uuid4()}.json"
        
        # Save files
        with open(ecc_path, 'wb') as f:
            content = await ecc_file.read()
            f.write(content)
        
        with open(ecp_path, 'wb') as f:
            content = await ecp_file.read()
            f.write(content)
        
        # Load data from uploaded files
        with open(ecc_path, 'r', encoding='utf-8') as f:
            ecc_data = json.load(f)
        
        with open(ecp_path, 'r', encoding='utf-8') as f:
            ecp_data = json.load(f)
        
        # Create temporary comparator with uploaded data
        temp_comparator = DataComparator()
        temp_comparator.cache['ECC_UPLOADED'] = ecc_data
        temp_comparator.cache['ECP_UPLOADED'] = ecp_data
        
        # Perform enhanced comparison
        result = temp_comparator.compare_datasets_with_mappings('ECC_UPLOADED', 'ECP_UPLOADED')
        
        # Clean up temp files
        ecc_path.unlink(missing_ok=True)
        ecp_path.unlink(missing_ok=True)
        
        return result
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Enhanced comparison error: {str(e)}")


@router.get("/datasets/{ecc_dataset}/{ecp_dataset}")
async def compare_existing_datasets(
    ecc_dataset: str,
    ecp_dataset: str
):
    """
    Enhanced comparison for existing datasets using mappings.
    
    Example: GET /compare/mapped/datasets/ECCSEP05/ECP_7
    
    Uses employee number mapping (PERNR_ECC_ECP.xlsx) and wage type 
    classification (wagetype_classification.xlsx) for intelligent comparison.
    
    Returns:
    - Matched records with ECC amount, ECP amount, and difference
    - Unmatched records from both systems
    - Wage type categorization
    - Summary statistics
    """
    try:
        result = comparator.compare_datasets_with_mappings(ecc_dataset, ecp_dataset)
        return result
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Enhanced comparison error: {str(e)}")


@router.get("/mappings")
async def get_mappings_info():
    """
    Get information about available mappings.
    
    Returns employee number mappings and wage type classifications.
    """
    try:
        emp_mapping = comparator.get_employee_mapping()
        wage_mapping = comparator.get_wage_type_mapping()
        
        wage_categories = list(set(wage_mapping.values()))
        
        return {
            'employee_mappings': {
                'count': len(emp_mapping),
                'description': 'ECC PERNR to ECP PERNR mapping',
                'sample': dict(list(emp_mapping.items())[:10])
            },
            'wage_type_mappings': {
                'count': len(wage_mapping),
                'categories': wage_categories,
                'category_count': len(wage_categories),
                'description': 'Wage Type to Category classification',
                'sample': dict(list(wage_mapping.items())[:10])
            }
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error loading mappings: {str(e)}")


@router.get("/export/{ecc_dataset}/{ecp_dataset}")
async def export_enhanced_comparison(
    ecc_dataset: str,
    ecp_dataset: str
):
    """
    Run enhanced comparison and save results to file.
    """
    try:
        result = comparator.compare_datasets_with_mappings(ecc_dataset, ecp_dataset)
        
        # Save to results directory
        results_dir = Path(__file__).parent.parent / 'data' / 'results'
        results_dir.mkdir(parents=True, exist_ok=True)
        
        filename = f"enhanced_comparison_{ecc_dataset}_vs_{ecp_dataset}_{uuid.uuid4().hex[:8]}.json"
        output_path = results_dir / filename
        
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(result, f, indent=2, default=str)
        
        return {
            'message': 'Enhanced comparison saved successfully',
            'filename': filename,
            'path': str(output_path),
            'summary': result['summary']
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Export error: {str(e)}")
