"""
Comparison logic for ECC vs ECP data files.
Identifies differences, missing records, and discrepancies.
"""

import json
from pathlib import Path
from typing import Dict, List, Any, Optional, Tuple
from collections import defaultdict
import hashlib
import pandas as pd


class DataComparator:
    """Compare two datasets and identify differences."""
    
    def __init__(self, data_dir: Path = None, mappings_dir: Path = None):
        """Initialize comparator with data and mappings directories."""
        if data_dir is None:
            data_dir = Path(__file__).parent.parent / "data" / "encoded"
        if mappings_dir is None:
            mappings_dir = Path(__file__).parent.parent / "data" / "realData"
            
        self.data_dir = Path(data_dir)
        self.mappings_dir = Path(mappings_dir)
        self.cache = {}
        self.mappings_cache = {}
    
    def load_mapping(self, mapping_file: str) -> pd.DataFrame:
        """Load mapping file from Excel."""
        if mapping_file in self.mappings_cache:
            return self.mappings_cache[mapping_file]
            
        mapping_path = self.mappings_dir / mapping_file
        if not mapping_path.exists():
            raise FileNotFoundError(f"Mapping file not found: {mapping_path}")
        
        if mapping_file.endswith('.xlsx'):
            df = pd.read_excel(mapping_path)
        else:
            df = pd.read_csv(mapping_path)
            
        self.mappings_cache[mapping_file] = df
        return df
    
    def get_employee_mapping(self) -> Dict[str, str]:
        """Get ECC to ECP employee number mapping."""
        df = self.load_mapping('PERNR_ECC_ECP.xlsx')
        # Create mapping dictionary: ECC_PERNR -> ECP_PERNR
        return dict(zip(df['ECC PERNR'].astype(str), df['ECP PERNR'].astype(str)))
    
    def get_wage_type_mapping(self) -> Dict[str, str]:
        """Get wage type to category mapping."""
        df = self.load_mapping('wagetype_classification.xlsx')
        # Create mapping dictionary: WT -> Categories
        return dict(zip(df['WT'].astype(str), df['Categories'].astype(str)))
    
    def load_dataset(self, dataset_name: str) -> Dict[str, Any]:
        """Load a JSON dataset from file."""
        if dataset_name in self.cache:
            return self.cache[dataset_name]
        
        json_path = self.data_dir / f"{dataset_name}.json"
        if not json_path.exists():
            raise FileNotFoundError(f"Dataset not found: {json_path}")
        
        with open(json_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        self.cache[dataset_name] = data
        return data
    
    def group_by_employee(self, records: List[Dict]) -> Dict[str, List[Dict]]:
        """Group records by employee ID."""
        grouped = defaultdict(list)
        for record in records:
            emp_id = record.get('Last name First name', '').strip()
            if emp_id:
                grouped[emp_id].append(record)
        return dict(grouped)
    
    def group_by_key(
        self, 
        records: List[Dict], 
        key_fields: List[str]
    ) -> Dict[str, Dict]:
        """
        Group records by composite key.
        
        Args:
            records: List of data records
            key_fields: List of field names to use as composite key
            
        Returns:
            Dictionary mapping composite key to record
        """
        grouped = {}
        for record in records:
            # Create composite key
            key_values = []
            for field in key_fields:
                value = record.get(field, '')
                if value is None:
                    value = ''
                key_values.append(str(value).strip())
            
            composite_key = '|'.join(key_values)
            
            # Store record (last one wins if duplicates)
            grouped[composite_key] = record
        
        return grouped
    
    def compare_datasets_with_mappings(
        self,
        ecc_dataset: str,
        ecp_dataset: str
    ) -> Dict[str, Any]:
        """
        Compare ECC and ECP datasets using employee and wage type mappings.
        Shows detailed comparison with amounts and differences.
        
        Args:
            ecc_dataset: ECC dataset name (e.g., 'ECC_MASTER')
            ecp_dataset: ECP dataset name (e.g., 'ECP_MASTER')
            
        Returns:
            Detailed comparison results with amounts and differences
        """
        # Load datasets
        ecc_data = self.load_dataset(ecc_dataset)
        ecp_data = self.load_dataset(ecp_dataset)
        
        # Load mappings
        emp_mapping = self.get_employee_mapping()
        wage_mapping = self.get_wage_type_mapping()
        
        ecc_records = ecc_data['data']
        ecp_records = ecp_data['data']
        
        print(f"Loaded {len(ecc_records)} ECC records and {len(ecp_records)} ECP records")
        print(f"Employee mappings: {len(emp_mapping)}")
        print(f"Wage type mappings: {len(wage_mapping)}")
        
        # Group records by employee and wage type
        ecc_grouped = self.group_by_employee_wage_type(ecc_records)
        ecp_grouped = self.group_by_employee_wage_type(ecp_records)
        
        # Perform comparison using mappings
        comparison_results = []
        summary_stats = {
            'total_ecc_records': len(ecc_records),
            'total_ecp_records': len(ecp_records),
            'mapped_employees': len(emp_mapping),
            'wage_type_categories': len(set(wage_mapping.values())),
            'matched_records': 0,
            'unmatched_ecc': 0,
            'unmatched_ecp': 0,
            'total_ecc_amount': 0.0,
            'total_ecp_amount': 0.0,
            'total_difference': 0.0
        }
        
        # Compare using employee mapping
        for ecc_emp, ecc_emp_records in ecc_grouped.items():
            # Map ECC employee to ECP employee
            ecp_emp = emp_mapping.get(str(ecc_emp))
            
            if ecp_emp and ecp_emp in ecp_grouped:
                # Employee exists in both systems
                ecp_emp_records = ecp_grouped[ecp_emp]
                
                # Compare wage types for this employee
                employee_comparison = self.compare_employee_wage_types(
                    ecc_emp, ecp_emp, ecc_emp_records, ecp_emp_records, wage_mapping
                )
                
                if employee_comparison['matches']:
                    comparison_results.extend(employee_comparison['matches'])
                    summary_stats['matched_records'] += len(employee_comparison['matches'])
                    
                    # Add amounts to totals
                    for match in employee_comparison['matches']:
                        summary_stats['total_ecc_amount'] += match.get('ecc_amount', 0)
                        summary_stats['total_ecp_amount'] += match.get('ecp_amount', 0)
                        summary_stats['total_difference'] += match.get('difference', 0)
                        
            else:
                # Employee only in ECC
                summary_stats['unmatched_ecc'] += len(ecc_emp_records)
                for record in ecc_emp_records[:5]:  # Sample first 5
                    comparison_results.append({
                        'ecc_employee': ecc_emp,
                        'ecp_employee': ecp_emp or 'Not Mapped',
                        'status': 'ECC Only',
                        'ecc_data': record,
                        'ecp_data': None,
                        'ecc_amount': self.extract_amount(record),
                        'ecp_amount': 0,
                        'difference': self.extract_amount(record),
                        'wage_type': record.get('WT', ''),
                        'wage_category': wage_mapping.get(record.get('WT', ''), 'Unknown')
                    })
        
        # Find ECP-only employees
        ecc_mapped_emps = set(emp_mapping.keys())
        for ecp_emp, ecp_emp_records in ecp_grouped.items():
            # Check if this ECP employee has a mapping back to ECC
            ecc_equivalent = None
            for ecc_id, ecp_id in emp_mapping.items():
                if str(ecp_id) == str(ecp_emp):
                    ecc_equivalent = ecc_id
                    break
            
            if not ecc_equivalent or ecc_equivalent not in ecc_grouped:
                # Employee only in ECP
                summary_stats['unmatched_ecp'] += len(ecp_emp_records)
                for record in ecp_emp_records[:5]:  # Sample first 5
                    comparison_results.append({
                        'ecc_employee': ecc_equivalent or 'Not Mapped',
                        'ecp_employee': ecp_emp,
                        'status': 'ECP Only',
                        'ecc_data': None,
                        'ecp_data': record,
                        'ecc_amount': 0,
                        'ecp_amount': self.extract_amount(record),
                        'difference': -self.extract_amount(record),
                        'wage_type': record.get('WT', ''),
                        'wage_category': wage_mapping.get(record.get('WT', ''), 'Unknown')
                    })
        
        return {
            'summary': summary_stats,
            'results': comparison_results[:1000],  # Limit to first 1000 results
            'mappings': {
                'employee_mapping_count': len(emp_mapping),
                'wage_type_mapping_count': len(wage_mapping),
                'wage_categories': list(set(wage_mapping.values()))
            }
        }
    
    def group_by_employee_wage_type(self, records: List[Dict]) -> Dict[str, Dict[str, List[Dict]]]:
        """Group records by employee, then by wage type."""
        grouped = defaultdict(lambda: defaultdict(list))
        
        for record in records:
            emp_id = str(record.get('Pers.No.', ''))
            wage_type = str(record.get('WT', ''))
            
            if emp_id:
                grouped[emp_id][wage_type].append(record)
        
        return dict(grouped)
    
    def compare_employee_wage_types(
        self, 
        ecc_emp: str, 
        ecp_emp: str, 
        ecc_records: Dict[str, List[Dict]], 
        ecp_records: Dict[str, List[Dict]],
        wage_mapping: Dict[str, str]
    ) -> Dict[str, Any]:
        """Compare wage types for a single employee across ECC and ECP."""
        matches = []
        
        # Get all wage types for this employee
        ecc_wage_types = set(ecc_records.keys())
        ecp_wage_types = set(ecp_records.keys())
        
        common_wage_types = ecc_wage_types & ecp_wage_types
        
        for wt in common_wage_types:
            ecc_wt_records = ecc_records[wt]
            ecp_wt_records = ecp_records[wt]
            
            # Sum amounts for this wage type
            ecc_amount = sum(self.extract_amount(r) for r in ecc_wt_records)
            ecp_amount = sum(self.extract_amount(r) for r in ecp_wt_records)
            difference = ecp_amount - ecc_amount
            
            # Use the first record from each for display
            ecc_sample = ecc_wt_records[0] if ecc_wt_records else {}
            ecp_sample = ecp_wt_records[0] if ecp_wt_records else {}
            
            matches.append({
                'ecc_employee': ecc_emp,
                'ecp_employee': ecp_emp,
                'status': 'Matched',
                'ecc_data': ecc_sample,
                'ecp_data': ecp_sample,
                'ecc_amount': ecc_amount,
                'ecp_amount': ecp_amount,
                'difference': difference,
                'wage_type': wt,
                'wage_category': wage_mapping.get(wt, 'Unknown'),
                'ecc_record_count': len(ecc_wt_records),
                'ecp_record_count': len(ecp_wt_records)
            })
        
        return {'matches': matches}
    
    def extract_amount(self, record: Dict) -> float:
        """Extract amount from a record, handling various formats."""
        amount_str = record.get('Amount', '0')
        if isinstance(amount_str, str):
            # Remove commas and handle parentheses for negative
            amount_str = amount_str.replace(',', '').replace('(', '-').replace(')', '')
            try:
                return float(amount_str)
            except ValueError:
                return 0.0
        return float(amount_str) if amount_str else 0.0
    
    def compare_records(
        self,
        record_a: Dict,
        record_b: Dict,
        value_fields: List[str] = None,
        tolerance: float = 0.01
    ) -> List[Dict]:
        """
        Compare two records field by field.
        
        Returns:
            List of differences found
        """
        differences = []
        
        # If no specific fields, compare all common fields
        if value_fields is None:
            value_fields = set(record_a.keys()) & set(record_b.keys())
        
        for field in value_fields:
            val_a = record_a.get(field)
            val_b = record_b.get(field)
            
            # Skip if both are None or empty
            if not val_a and not val_b:
                continue
            
            # Compare values
            if not self.values_equal(val_a, val_b, tolerance):
                differences.append({
                    'field': field,
                    'value_a': val_a,
                    'value_b': val_b,
                    'diff_type': self.classify_difference(val_a, val_b)
                })
        
        return differences
    
    def values_equal(self, val_a: Any, val_b: Any, tolerance: float) -> bool:
        """Check if two values are equal within tolerance."""
        # Handle None/empty
        if val_a is None and val_b is None:
            return True
        if val_a is None or val_b is None:
            return False
        
        # Try numeric comparison
        try:
            num_a = float(str(val_a).replace(',', ''))
            num_b = float(str(val_b).replace(',', ''))
            return abs(num_a - num_b) <= tolerance
        except (ValueError, TypeError):
            pass
        
        # String comparison (case-insensitive, stripped)
        str_a = str(val_a).strip().lower()
        str_b = str(val_b).strip().lower()
        return str_a == str_b
    
    def classify_difference(self, val_a: Any, val_b: Any) -> str:
        """Classify the type of difference between two values."""
        if val_a is None or val_a == '':
            return 'missing_in_a'
        if val_b is None or val_b == '':
            return 'missing_in_b'
        
        # Check if numeric
        try:
            float(str(val_a).replace(',', ''))
            float(str(val_b).replace(',', ''))
            return 'numeric_difference'
        except (ValueError, TypeError):
            pass
        
        return 'value_mismatch'
    
    def get_employee_comparison(
        self,
        dataset_a: str,
        dataset_b: str,
        employee_id: str
    ) -> Dict[str, Any]:
        """
        Compare all records for a specific employee across two datasets.
        
        Args:
            dataset_a: First dataset name
            dataset_b: Second dataset name
            employee_id: Employee ID to compare
            
        Returns:
            Comparison results for the employee
        """
        data_a = self.load_dataset(dataset_a)
        data_b = self.load_dataset(dataset_b)
        
        # Filter records for this employee
        records_a = [r for r in data_a['data'] 
                     if r.get('Last name First name', '').strip() == employee_id]
        records_b = [r for r in data_b['data']
                     if r.get('Last name First name', '').strip() == employee_id]
        
        if not records_a and not records_b:
            return {
                'error': f'Employee {employee_id} not found in either dataset',
                'employee_id': employee_id
            }
        
        # Compare using standard comparison
        result = self.compare_datasets(
            dataset_a,
            dataset_b,
            key_fields=['Pers.No.', 'For-period', 'WT']
        )
        
        # Filter results for this employee
        filtered_result = {
            'employee_id': employee_id,
            'records_in_a': len(records_a),
            'records_in_b': len(records_b),
            'sample_records_a': records_a[:10],
            'sample_records_b': records_b[:10]
        }
        
        return filtered_result
