"""
Dask-based comparison logic for large-scale ECC vs ECP data files.
Optimized for memory efficiency and parallel processing.
"""

import json
from pathlib import Path
from typing import Dict, List, Any, Optional
import pandas as pd
import dask.dataframe as dd
from dask.distributed import Client, LocalCluster
from dask import delayed
import numpy as np


class DaskDataComparator:
    """Compare two large datasets using Dask for parallel processing."""
    
    def __init__(self, data_dir: Path = None, mappings_dir: Path = None, n_workers: int = 4):
        """Initialize Dask comparator with data and mappings directories."""
        if data_dir is None:
            data_dir = Path(__file__).parent.parent / "data" / "encoded"
        if mappings_dir is None:
            mappings_dir = Path(__file__).parent.parent / "data" / "realData"
            
        self.data_dir = Path(data_dir)
        self.mappings_dir = Path(mappings_dir)
        self.n_workers = n_workers
        self.client = None
        self.mappings_cache = {}
        
    def start_cluster(self):
        """Start Dask local cluster for parallel processing."""
        if self.client is None:
            cluster = LocalCluster(n_workers=self.n_workers, threads_per_worker=2, memory_limit='2GB')
            self.client = Client(cluster)
            print(f"Dask cluster started: {self.client.dashboard_link}")
        return self.client
    
    def stop_cluster(self):
        """Stop Dask cluster."""
        if self.client:
            self.client.close()
            self.client = None
    
    def load_mappings(self) -> tuple[pd.DataFrame, pd.DataFrame]:
        """Load employee and wage type mappings from Excel files."""
        if 'employee' not in self.mappings_cache:
            emp_df = pd.read_excel(self.mappings_dir / 'PERNR_ECC_ECP.xlsx', dtype=str)
            emp_df.columns = [c.strip() for c in emp_df.columns]
            self.mappings_cache['employee'] = emp_df
            
        if 'wage_type' not in self.mappings_cache:
            wt_df = pd.read_excel(self.mappings_dir / 'wagetype_classification.xlsx', dtype=str)
            wt_df.columns = [c.strip() for c in wt_df.columns]
            self.mappings_cache['wage_type'] = wt_df
            
        return self.mappings_cache['employee'], self.mappings_cache['wage_type']
    
    def normalize_amount_column(self, df: dd.DataFrame, col: str = 'Amount') -> dd.Series:
        """
        Vectorized cleanup of amount column: remove commas, handle parentheses for negatives.
        
        Args:
            df: Dask DataFrame
            col: Column name containing amount values
            
        Returns:
            Dask Series with normalized float amounts
        """
        # Convert to string and clean
        s = df[col].astype(str)
        # Remove commas
        s = s.str.replace(',', '', regex=False)
        # Handle parentheses as negative (accounting format)
        s = s.str.replace('(', '-', regex=False)
        s = s.str.replace(')', '', regex=False)
        # Convert to numeric, coerce errors to 0
        return dd.to_numeric(s, errors='coerce').fillna(0.0)
    
    def load_dask_dataset(self, dataset_name: str) -> dd.DataFrame:
        """
        Load dataset as Dask DataFrame.
        Supports JSON format with chunked reading.
        
        Args:
            dataset_name: Name of the dataset (without extension)
            
        Returns:
            Dask DataFrame
        """
        json_path = self.data_dir / f"{dataset_name}.json"
        
        if not json_path.exists():
            raise FileNotFoundError(f"Dataset not found: {json_path}")
        
        # Load JSON file metadata first
        with open(json_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        # Convert to pandas DataFrame then to Dask
        # For very large files, consider writing to Parquet first
        records = data.get('data', [])
        pdf = pd.DataFrame(records)
        
        # Convert to Dask DataFrame with partitions
        npartitions = max(1, len(records) // 10000)  # ~10k rows per partition
        ddf = dd.from_pandas(pdf, npartitions=npartitions)
        
        return ddf
    
    def compare_datasets_aggregated(
        self,
        ecc_dataset: str,
        ecp_dataset: str,
        page: int = 1,
        page_size: int = 100
    ) -> Dict[str, Any]:
        """
        Dask-based comparison using aggregation strategy.
        Groups by employee and wage type, then compares sums.
        
        Args:
            ecc_dataset: ECC dataset name
            ecp_dataset: ECP dataset name
            page: Page number for pagination (1-indexed)
            page_size: Number of results per page
            
        Returns:
            Comparison results with summary and paginated data
        """
        # Start cluster
        self.start_cluster()
        
        try:
            # Load mappings
            emp_df, wt_df = self.load_mappings()
            
            # Load datasets as Dask DataFrames
            print(f"Loading datasets: {ecc_dataset}, {ecp_dataset}")
            ecc_ddf = self.load_dask_dataset(ecc_dataset)
            ecp_ddf = self.load_dask_dataset(ecp_dataset)
            
            # Normalize column names
            ecc_ddf.columns = [c.strip() for c in ecc_ddf.columns]
            ecp_ddf.columns = [c.strip() for c in ecp_ddf.columns]
            
            # Parse amounts
            print("Parsing amounts...")
            ecc_ddf = ecc_ddf.assign(amount=self.normalize_amount_column(ecc_ddf, 'Amount'))
            ecp_ddf = ecp_ddf.assign(amount=self.normalize_amount_column(ecp_ddf, 'Amount'))
            
            # Ensure employee ID and WT columns are strings and trimmed
            ecc_ddf['Pers.No.'] = ecc_ddf['Pers.No.'].astype(str).str.strip()
            ecc_ddf['WT'] = ecc_ddf['WT'].astype(str).str.strip()
            ecp_ddf['Pers.No.'] = ecp_ddf['Pers.No.'].astype(str).str.strip()
            ecp_ddf['WT'] = ecp_ddf['WT'].astype(str).str.strip()
            
            # Create employee mapping dictionary
            emp_map = emp_df.set_index('ECC PERNR')['ECP PERNR'].to_dict()
            
            # Map ECC employee IDs to ECP employee IDs
            print("Mapping employee IDs...")
            ecc_ddf = ecc_ddf.assign(mapped_ecp=ecc_ddf['Pers.No.'].map(emp_map))
            
            # Aggregate by mapped employee ID and wage type
            print("Aggregating ECC data...")
            ecc_agg = (ecc_ddf
                      .dropna(subset=['mapped_ecp'])
                      .groupby(['mapped_ecp', 'WT'])['amount']
                      .sum()
                      .reset_index()
                      .rename(columns={'mapped_ecp': 'ecp_id', 'amount': 'ecc_amount'}))
            
            print("Aggregating ECP data...")
            ecp_agg = (ecp_ddf
                      .groupby(['Pers.No.', 'WT'])['amount']
                      .sum()
                      .reset_index()
                      .rename(columns={'Pers.No.': 'ecp_id', 'amount': 'ecp_amount'}))
            
            # Perform outer join to find matches and differences
            print("Merging aggregated datasets...")
            merged = ecc_agg.merge(ecp_agg, on=['ecp_id', 'WT'], how='outer').fillna(0)
            
            # Calculate difference
            merged = merged.assign(difference=merged['ecp_amount'] - merged['ecc_amount'])
            
            # Add wage type category mapping
            wt_map = wt_df.set_index('WT')['Categories'].to_dict()
            merged = merged.assign(wage_category=merged['WT'].map(wt_map))
            
            # Add status column
            def determine_status(row):
                if row['ecc_amount'] > 0 and row['ecp_amount'] > 0:
                    return 'Matched'
                elif row['ecc_amount'] > 0:
                    return 'ECC Only'
                else:
                    return 'ECP Only'
            
            merged = merged.assign(status=merged.apply(determine_status, axis=1, meta=('status', 'object')))
            
            # Compute summary statistics
            print("Computing summary...")
            summary_stats = {
                'total_rows': int(merged.shape[0].compute()),
                'total_ecc_amount': float(merged['ecc_amount'].sum().compute()),
                'total_ecp_amount': float(merged['ecp_amount'].sum().compute()),
                'total_difference': float(merged['difference'].sum().compute()),
                'matched_count': int((merged['status'] == 'Matched').sum().compute()),
                'ecc_only_count': int((merged['status'] == 'ECC Only').sum().compute()),
                'ecp_only_count': int((merged['status'] == 'ECP Only').sum().compute())
            }
            
            # Sort by absolute difference (largest first)
            merged = merged.assign(abs_diff=merged['difference'].abs())
            merged = merged.sort_values('abs_diff', ascending=False)
            
            # Compute all results first (needed for proper pagination)
            print(f"Computing full results...")
            merged_computed = merged.compute()
            
            # Reset index for proper integer-based indexing
            merged_computed = merged_computed.reset_index(drop=True)
            
            # Pagination
            start_idx = (page - 1) * page_size
            end_idx = start_idx + page_size
            
            # Get paginated results
            print(f"Fetching page {page} (rows {start_idx}-{end_idx})...")
            paginated = merged_computed.iloc[start_idx:end_idx]
            
            # Convert to records
            results = paginated.drop(columns=['abs_diff']).to_dict('records')
            
            # Calculate pagination info
            total_pages = (summary_stats['total_rows'] + page_size - 1) // page_size
            
            return {
                'summary': summary_stats,
                'results': results,
                'pagination': {
                    'page': page,
                    'page_size': page_size,
                    'total_rows': summary_stats['total_rows'],
                    'total_pages': total_pages,
                    'has_next': page < total_pages,
                    'has_prev': page > 1
                },
                'mappings': {
                    'employee_mapping_count': len(emp_map),
                    'wage_type_mapping_count': len(wt_map),
                    'wage_categories': list(set(wt_map.values()))
                }
            }
            
        except Exception as e:
            print(f"Error in Dask comparison: {str(e)}")
            raise
        finally:
            # Keep cluster alive for subsequent requests
            pass
    
    def export_full_comparison(
        self,
        ecc_dataset: str,
        ecp_dataset: str,
        output_format: str = 'parquet'
    ) -> Path:
        """
        Export full comparison results to Parquet or JSON.
        
        Args:
            ecc_dataset: ECC dataset name
            ecp_dataset: ECP dataset name
            output_format: 'parquet' or 'json'
            
        Returns:
            Path to output file/directory
        """
        self.start_cluster()
        
        try:
            # Reuse comparison logic but compute all results
            result = self.compare_datasets_aggregated(ecc_dataset, ecp_dataset, page=1, page_size=999999999)
            
            # Export directory
            export_dir = self.data_dir.parent / 'results' / f'dask_comparison_{ecc_dataset}_vs_{ecp_dataset}'
            export_dir.mkdir(parents=True, exist_ok=True)
            
            if output_format == 'parquet':
                output_path = export_dir / 'comparison.parquet'
                # Convert results back to DataFrame and save as Parquet
                df = pd.DataFrame(result['results'])
                df.to_parquet(output_path, index=False)
            else:  # json
                output_path = export_dir / 'comparison.json'
                with open(output_path, 'w', encoding='utf-8') as f:
                    json.dump(result, f, indent=2, default=str)
            
            return output_path
            
        finally:
            pass
    
    def __del__(self):
        """Cleanup: close Dask cluster on deletion."""
        self.stop_cluster()
