"""
Comparison logic for comparing two datasets
"""
import json
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any
import pandas as pd
from datetime import datetime


class DatasetComparator:
    """Compare two parsed datasets and generate difference reports"""
    
    def __init__(self, dataset_a_path: Path, dataset_b_path: Path):
        self.dataset_a_path = dataset_a_path
        self.dataset_b_path = dataset_b_path
        self.data_a = None
        self.data_b = None
        self.df_a = None
        self.df_b = None
        
    def load_datasets(self):
        """Load both JSON datasets"""
        with open(self.dataset_a_path, 'r') as f:
            self.data_a = json.load(f)
        with open(self.dataset_b_path, 'r') as f:
            self.data_b = json.load(f)
            
        # Convert to DataFrames
        self.df_a = pd.DataFrame(self.data_a['data'])
        self.df_b = pd.DataFrame(self.data_b['data'])
        
        return self
    
    def compare(
        self,
        key_columns: List[str],
        compare_columns: Optional[List[str]] = None,
        tolerance: float = 0.01,
        ignore_case: bool = True
    ) -> Dict[str, Any]:
        """
        Compare two datasets based on key columns
        
        Args:
            key_columns: Columns to use as unique identifiers
            compare_columns: Specific columns to compare (None = all columns)
            tolerance: Numeric tolerance for floating point comparisons
            ignore_case: Whether to ignore case in string comparisons
        
        Returns:
            Dictionary with comparison results
        """
        if self.df_a is None or self.df_b is None:
            self.load_datasets()
        
        # Validate key columns exist
        for col in key_columns:
            if col not in self.df_a.columns:
                raise ValueError(f"Key column '{col}' not found in Dataset A")
            if col not in self.df_b.columns:
                raise ValueError(f"Key column '{col}' not found in Dataset B")
        
        # Determine columns to compare
        if compare_columns is None:
            # Compare all columns except keys
            compare_columns = [col for col in self.df_a.columns if col not in key_columns]
        
        # Create composite keys for matching
        self.df_a['_composite_key'] = self.df_a[key_columns].astype(str).agg('||'.join, axis=1)
        self.df_b['_composite_key'] = self.df_b[key_columns].astype(str).agg('||'.join, axis=1)
        
        # Find matching and non-matching records
        keys_a = set(self.df_a['_composite_key'])
        keys_b = set(self.df_b['_composite_key'])
        
        common_keys = keys_a & keys_b
        only_in_a = keys_a - keys_b
        only_in_b = keys_b - keys_a
        
        # Compare common records
        differences = []
        identical = []
        
        for key in common_keys:
            record_a = self.df_a[self.df_a['_composite_key'] == key].iloc[0]
            record_b = self.df_b[self.df_b['_composite_key'] == key].iloc[0]
            
            diff_fields = []
            
            for col in compare_columns:
                if col not in self.df_a.columns or col not in self.df_b.columns:
                    continue
                
                val_a = record_a[col]
                val_b = record_b[col]
                
                # Compare values based on type
                if self._values_differ(val_a, val_b, tolerance, ignore_case):
                    diff_fields.append({
                        'column': col,
                        'value_a': self._serialize_value(val_a),
                        'value_b': self._serialize_value(val_b)
                    })
            
            if diff_fields:
                differences.append({
                    'key': key,
                    'key_values': {k: self._serialize_value(record_a[k]) for k in key_columns},
                    'differences': diff_fields
                })
            else:
                identical.append({
                    'key': key,
                    'key_values': {k: self._serialize_value(record_a[k]) for k in key_columns}
                })
        
        # Get samples of records only in A or B
        only_a_records = []
        for key in list(only_in_a)[:100]:  # Limit to first 100
            record = self.df_a[self.df_a['_composite_key'] == key].iloc[0]
            only_a_records.append({
                'key': key,
                'key_values': {k: self._serialize_value(record[k]) for k in key_columns},
                'sample_data': {col: self._serialize_value(record[col]) for col in compare_columns[:5]}
            })
        
        only_b_records = []
        for key in list(only_in_b)[:100]:  # Limit to first 100
            record = self.df_b[self.df_b['_composite_key'] == key].iloc[0]
            only_b_records.append({
                'key': key,
                'key_values': {k: self._serialize_value(record[k]) for k in key_columns},
                'sample_data': {col: self._serialize_value(record[col]) for col in compare_columns[:5]}
            })
        
        return {
            'comparison_id': datetime.now().strftime('%Y%m%d_%H%M%S'),
            'dataset_a': {
                'name': self.dataset_a_path.stem,
                'total_records': len(self.df_a),
                'metadata': self.data_a.get('metadata', {})
            },
            'dataset_b': {
                'name': self.dataset_b_path.stem,
                'total_records': len(self.df_b),
                'metadata': self.data_b.get('metadata', {})
            },
            'parameters': {
                'key_columns': key_columns,
                'compare_columns': compare_columns,
                'tolerance': tolerance,
                'ignore_case': ignore_case
            },
            'summary': {
                'total_in_a': len(keys_a),
                'total_in_b': len(keys_b),
                'common_records': len(common_keys),
                'only_in_a': len(only_in_a),
                'only_in_b': len(only_in_b),
                'identical_records': len(identical),
                'records_with_differences': len(differences),
                'match_rate': round(len(common_keys) / max(len(keys_a), len(keys_b)) * 100, 2)
            },
            'differences': differences[:500],  # Limit to first 500 differences
            'identical_sample': identical[:50],  # Sample of identical records
            'only_in_a': only_a_records,
            'only_in_b': only_b_records,
            'notes': {
                'differences_shown': min(500, len(differences)),
                'total_differences': len(differences),
                'only_in_a_shown': min(100, len(only_in_a)),
                'only_in_b_shown': min(100, len(only_in_b))
            }
        }
    
    def _values_differ(self, val_a, val_b, tolerance: float, ignore_case: bool) -> bool:
        """Check if two values are different"""
        # Handle NaN/None
        if pd.isna(val_a) and pd.isna(val_b):
            return False
        if pd.isna(val_a) or pd.isna(val_b):
            return True
        
        # Numeric comparison
        if isinstance(val_a, (int, float)) and isinstance(val_b, (int, float)):
            return abs(float(val_a) - float(val_b)) > tolerance
        
        # String comparison
        if isinstance(val_a, str) and isinstance(val_b, str):
            if ignore_case:
                return val_a.lower().strip() != val_b.lower().strip()
            return val_a.strip() != val_b.strip()
        
        # Default comparison
        return val_a != val_b
    
    def _serialize_value(self, val):
        """Convert value to JSON-serializable format"""
        if pd.isna(val):
            return None
        if isinstance(val, (pd.Timestamp, datetime)):
            return val.isoformat()
        return val
    
    def export_comparison(self, result: Dict[str, Any], output_path: Path):
        """Export comparison results to JSON file"""
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        with open(output_path, 'w') as f:
            json.dump(result, f, indent=2)
        
        return output_path


def quick_compare(
    dataset_a: str,
    dataset_b: str,
    key_columns: List[str],
    compare_columns: Optional[List[str]] = None,
    tolerance: float = 0.01,
    ignore_case: bool = True
) -> Dict[str, Any]:
    """
    Quick comparison function for API endpoints
    
    Args:
        dataset_a: Name of dataset A (without .json extension)
        dataset_b: Name of dataset B (without .json extension)
        key_columns: Columns to use as unique identifiers
        compare_columns: Specific columns to compare
        tolerance: Numeric tolerance
        ignore_case: Ignore case in strings
    
    Returns:
        Comparison results dictionary
    """
    base_path = Path(__file__).parent.parent / 'data' / 'encoded'
    
    path_a = base_path / f"{dataset_a}.json"
    path_b = base_path / f"{dataset_b}.json"
    
    if not path_a.exists():
        raise FileNotFoundError(f"Dataset A not found: {path_a}")
    if not path_b.exists():
        raise FileNotFoundError(f"Dataset B not found: {path_b}")
    
    comparator = DatasetComparator(path_a, path_b)
    result = comparator.compare(
        key_columns=key_columns,
        compare_columns=compare_columns,
        tolerance=tolerance,
        ignore_case=ignore_case
    )
    
    return result


if __name__ == '__main__':
    # Example usage
    print("Testing comparison logic...")
    
    result = quick_compare(
        dataset_a='ECCSEP05',
        dataset_b='ECP_7',
        key_columns=['Pers.No.', 'For-period', 'WT'],
        tolerance=0.01,
        ignore_case=True
    )
    
    print(f"\nComparison Summary:")
    print(f"  Dataset A: {result['dataset_a']['name']} ({result['dataset_a']['total_records']} records)")
    print(f"  Dataset B: {result['dataset_b']['name']} ({result['dataset_b']['total_records']} records)")
    print(f"  Common records: {result['summary']['common_records']}")
    print(f"  Only in A: {result['summary']['only_in_a']}")
    print(f"  Only in B: {result['summary']['only_in_b']}")
    print(f"  Identical: {result['summary']['identical_records']}")
    print(f"  With differences: {result['summary']['records_with_differences']}")
    print(f"  Match rate: {result['summary']['match_rate']}%")
