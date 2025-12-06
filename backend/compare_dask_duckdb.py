"""
Enhanced Dask comparator with DuckDB streaming for progressive results.
Supports background processing with incremental result availability.
"""

import duckdb
import pandas as pd
import dask.dataframe as dd
from pathlib import Path
from typing import Dict, Any, Optional, Callable
from datetime import datetime

from .compare_dask import DaskDataComparator
from .job_manager import JobManager, JobStatus


class DaskDuckDBComparator(DaskDataComparator):
    """
    Enhanced comparator that streams results to DuckDB for fast queries.
    Enables progressive loading and complex sorting without computing full dataset.
    """
    
    def __init__(self, data_dir: Path = None, mappings_dir: Path = None, n_workers: int = 4):
        """Initialize comparator with DuckDB support."""
        super().__init__(data_dir, mappings_dir, n_workers)
        self.job_manager = JobManager()
    
    def compare_datasets_async(
        self,
        ecc_dataset: str,
        ecp_dataset: str,
        progress_callback: Optional[Callable[[str, float], None]] = None,
        job_id: Optional[str] = None
    ) -> str:
        """
        Start async comparison that streams results to DuckDB.
        
        Args:
            ecc_dataset: ECC dataset name
            ecp_dataset: ECP dataset name
            progress_callback: Optional callback(message, progress_pct)
            job_id: Optional existing job_id (creates new if not provided)
            
        Returns:
            job_id: Unique job identifier for tracking
        """
        # Create job or use existing
        if job_id is None:
            job_id = self.job_manager.create_job(ecc_dataset, ecp_dataset)
        db_path = self.job_manager.get_result_db_path(job_id)
        
        def update_progress(message: str, progress: float, status: Optional[JobStatus] = None):
            """Update job progress."""
            self.job_manager.update_job(
                job_id=job_id,
                status=status,
                progress=progress,
                progress_message=message
            )
            if progress_callback:
                progress_callback(message, progress)
        
        try:
            # Start cluster
            self.start_cluster()
            
            update_progress("Loading mappings...", 5, JobStatus.LOADING_DATA)
            
            # Load mappings
            emp_df, wt_df = self.load_mappings()
            emp_map = emp_df.set_index('ECC PERNR')['ECP PERNR'].to_dict()
            wt_map = wt_df.set_index('WT')['Categories'].to_dict()
            
            update_progress("Loading datasets...", 10, JobStatus.LOADING_DATA)
            
            # Load datasets
            ecc_ddf = self.load_dask_dataset(ecc_dataset)
            ecp_ddf = self.load_dask_dataset(ecp_dataset)
            
            # Normalize columns
            ecc_ddf.columns = [c.strip() for c in ecc_ddf.columns]
            ecp_ddf.columns = [c.strip() for c in ecp_ddf.columns]
            
            update_progress("Parsing amounts...", 15)
            
            # Parse amounts
            ecc_ddf = ecc_ddf.assign(amount=self.normalize_amount_column(ecc_ddf, 'Amount'))
            ecp_ddf = ecp_ddf.assign(amount=self.normalize_amount_column(ecp_ddf, 'Amount'))
            
            # Ensure columns are strings
            ecc_ddf['Pers.No.'] = ecc_ddf['Pers.No.'].astype(str).str.strip()
            ecc_ddf['WT'] = ecc_ddf['WT'].astype(str).str.strip()
            ecp_ddf['Pers.No.'] = ecp_ddf['Pers.No.'].astype(str).str.strip()
            ecp_ddf['WT'] = ecp_ddf['WT'].astype(str).str.strip()
            
            update_progress("Mapping employee IDs...", 25, JobStatus.MAPPING)
            
            # Map ECC employee IDs
            ecc_ddf = ecc_ddf.assign(mapped_ecp=ecc_ddf['Pers.No.'].map(emp_map))
            
            update_progress("Aggregating ECC data...", 35, JobStatus.AGGREGATING)
            
            # Aggregate ECC
            ecc_agg = (ecc_ddf
                      .dropna(subset=['mapped_ecp'])
                      .groupby(['mapped_ecp', 'WT'])['amount']
                      .sum()
                      .reset_index()
                      .rename(columns={'mapped_ecp': 'ecp_id', 'amount': 'ecc_amount'}))
            
            update_progress("Aggregating ECP data...", 50, JobStatus.AGGREGATING)
            
            # Aggregate ECP
            ecp_agg = (ecp_ddf
                      .groupby(['Pers.No.', 'WT'])['amount']
                      .sum()
                      .reset_index()
                      .rename(columns={'Pers.No.': 'ecp_id', 'amount': 'ecp_amount'}))
            
            update_progress("Merging datasets...", 65, JobStatus.MERGING)
            
            # Perform outer join
            merged = ecc_agg.merge(ecp_agg, on=['ecp_id', 'WT'], how='outer').fillna(0)
            
            # Calculate difference
            merged = merged.assign(difference=merged['ecp_amount'] - merged['ecc_amount'])
            
            # Rename WT column before further processing
            merged = merged.rename(columns={'WT': 'wage_type'})
            
            # Add wage type category and fill NaN with "Uncategorized"
            merged = merged.assign(wage_category=merged['wage_type'].map(wt_map).fillna('Uncategorized'))
            
            # Add status
            def determine_status(row):
                if row['ecc_amount'] > 0 and row['ecp_amount'] > 0:
                    return 'Matched'
                elif row['ecc_amount'] > 0:
                    return 'ECC Only'
                else:
                    return 'ECP Only'
            
            merged = merged.assign(status=merged.apply(determine_status, axis=1, meta=('status', 'object')))
            
            update_progress("Computing results...", 75, JobStatus.STORING)
            
            # Compute the full results
            result_df = merged.compute()
            
            # Get total row count
            total_rows = len(result_df)
            
            update_progress(f"Storing {total_rows:,} rows to DuckDB...", 85, JobStatus.STORING)
            
            # Create DuckDB connection and store results
            con = duckdb.connect(str(db_path))
            
            # Create table with proper types
            con.execute("""
                CREATE TABLE comparison_results (
                    ecp_id VARCHAR,
                    wage_type VARCHAR,
                    wage_category VARCHAR,
                    ecc_amount DOUBLE,
                    ecp_amount DOUBLE,
                    difference DOUBLE,
                    status VARCHAR
                )
            """)
            
            # Ensure columns are in the correct order
            result_df = result_df[['ecp_id', 'wage_type', 'wage_category', 'ecc_amount', 'ecp_amount', 'difference', 'status']]
            
            # Insert data
            con.execute("INSERT INTO comparison_results SELECT * FROM result_df")
            
            # Create indexes for faster queries
            con.execute("CREATE INDEX idx_ecp_id ON comparison_results(ecp_id)")
            con.execute("CREATE INDEX idx_category ON comparison_results(wage_category)")
            con.execute("CREATE INDEX idx_status ON comparison_results(status)")
            
            # Get summary statistics
            summary_stats = con.execute("""
                SELECT 
                    COUNT(*) as total_rows,
                    SUM(ecc_amount) as total_ecc_amount,
                    SUM(ecp_amount) as total_ecp_amount,
                    SUM(CASE WHEN status = 'Matched' THEN 1 ELSE 0 END) as matched_count,
                    SUM(CASE WHEN status = 'ECC Only' THEN 1 ELSE 0 END) as ecc_only_count,
                    SUM(CASE WHEN status = 'ECP Only' THEN 1 ELSE 0 END) as ecp_only_count
                FROM comparison_results
            """).fetchone()
            
            con.close()
            
            update_progress("Comparison complete!", 100, JobStatus.COMPLETED)
            
            # Update job with results
            self.job_manager.update_job(
                job_id=job_id,
                total_rows=total_rows,
                processed_rows=total_rows,
                result_path=str(db_path),
                metadata={
                    "summary": {
                        "total_rows": int(summary_stats[0]),
                        "total_ecc_amount": float(summary_stats[1] or 0),
                        "total_ecp_amount": float(summary_stats[2] or 0),
                        "matched_count": int(summary_stats[3] or 0),
                        "ecc_only_count": int(summary_stats[4] or 0),
                        "ecp_only_count": int(summary_stats[5] or 0)
                    },
                    "employee_mapping_count": len(emp_map),
                    "wage_type_mapping_count": len(wt_map)
                }
            )
            
            return job_id
            
        except Exception as e:
            error_msg = str(e)
            self.job_manager.update_job(
                job_id=job_id,
                error=error_msg
            )
            raise
        finally:
            pass  # Keep cluster alive for subsequent requests
    
    def query_results(
        self,
        job_id: str,
        page: int = 1,
        page_size: int = 100,
        sort_by: str = "ecp_id,wage_category,ecc_amount DESC,ecp_amount DESC",
        filter_status: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Query results from DuckDB with sorting and pagination.
        
        Args:
            job_id: Job identifier
            page: Page number (1-indexed)
            page_size: Results per page
            sort_by: Sort specification (e.g., "ecp_id,wage_category,ecc_amount DESC")
            filter_status: Optional filter by status (Matched, ECC Only, ECP Only)
            
        Returns:
            Dictionary with results and pagination info
        """
        job = self.job_manager.get_job(job_id)
        if not job:
            raise ValueError(f"Job {job_id} not found")
        
        if job["status"] != JobStatus.COMPLETED:
            return {
                "status": job["status"],
                "progress": job["progress"],
                "progress_message": job["progress_message"],
                "results": [],
                "summary": job.get("metadata", {}).get("summary", {}),
                "pagination": {
                    "page": page,
                    "page_size": page_size,
                    "total_rows": 0,
                    "total_pages": 0
                }
            }
        
        db_path = job["result_path"]
        if not db_path or not Path(db_path).exists():
            raise ValueError(f"Result database not found for job {job_id}")
        
        # Connect to DuckDB
        con = duckdb.connect(str(db_path), read_only=True)
        
        try:
            # Build query
            where_clause = ""
            if filter_status:
                where_clause = f"WHERE status = '{filter_status}'"
            
            # Parse sort specification
            order_clause = self._build_order_clause(sort_by)
            
            # Get total count
            total_rows = con.execute(f"""
                SELECT COUNT(*) FROM comparison_results {where_clause}
            """).fetchone()[0]
            
            # Calculate pagination
            total_pages = (total_rows + page_size - 1) // page_size
            offset = (page - 1) * page_size
            
            # Query paginated results
            query = f"""
                SELECT 
                    ecp_id,
                    wage_type,
                    wage_category,
                    ecc_amount,
                    ecp_amount,
                    difference,
                    status
                FROM comparison_results
                {where_clause}
                {order_clause}
                LIMIT {page_size} OFFSET {offset}
            """
            
            results_df = con.execute(query).fetchdf()
            
            # Convert to records
            results = results_df.to_dict('records')
            
            return {
                "status": "completed",
                "results": results,
                "summary": job["metadata"].get("summary", {}),
                "pagination": {
                    "page": page,
                    "page_size": page_size,
                    "total_rows": total_rows,
                    "total_pages": total_pages,
                    "has_next": page < total_pages,
                    "has_prev": page > 1
                }
            }
            
        finally:
            con.close()
    
    def _build_order_clause(self, sort_by: str) -> str:
        """
        Build SQL ORDER BY clause from sort specification.
        
        Args:
            sort_by: Comma-separated sort fields (e.g., "ecp_id,wage_category,ecc_amount DESC")
            
        Returns:
            SQL ORDER BY clause
        """
        if not sort_by:
            return ""
        
        sort_fields = []
        for field in sort_by.split(','):
            field = field.strip()
            if ' DESC' in field.upper():
                col = field.upper().replace(' DESC', '').strip()
                sort_fields.append(f"{col} DESC")
            elif ' ASC' in field.upper():
                col = field.upper().replace(' ASC', '').strip()
                sort_fields.append(f"{col} ASC")
            else:
                sort_fields.append(f"{field} ASC")
        
        return f"ORDER BY {', '.join(sort_fields)}"
