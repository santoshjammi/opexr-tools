#!/usr/bin/env python3
"""
Test script for async DuckDB comparison.
Tests the complete flow: start job -> monitor progress -> query results
"""

import sys
sys.path.insert(0, '.')

from backend.compare_dask_duckdb import DaskDuckDBComparator
from backend.job_manager import JobManager
import time

def test_async_comparison():
    """Test async comparison with progress tracking."""
    
    print("=" * 70)
    print("TESTING ASYNC DUCKDB COMPARISON")
    print("=" * 70)
    print()
    
    # Initialize
    comparator = DaskDuckDBComparator()
    job_manager = JobManager()
    
    # Progress callback
    def progress_callback(message: str, progress: float):
        bar_length = 50
        filled = int(bar_length * progress / 100)
        bar = '█' * filled + '░' * (bar_length - filled)
        print(f"\r[{bar}] {progress:.1f}% - {message}", end='', flush=True)
    
    try:
        print("Starting comparison...")
        print()
        
        # Start comparison
        job_id = comparator.compare_datasets_async(
            ecc_dataset="ECCSEP01",
            ecp_dataset="ECP_1",
            progress_callback=progress_callback
        )
        
        print()
        print()
        print(f"✓ Comparison completed! Job ID: {job_id}")
        print()
        
        # Get job summary
        job = job_manager.get_job(job_id)
        summary = job["metadata"]["summary"]
        
        print("=" * 70)
        print("SUMMARY")
        print("=" * 70)
        print(f"Total rows: {summary['total_rows']:,}")
        print(f"Matched: {summary['matched_count']:,}")
        print(f"ECC Only: {summary['ecc_only_count']:,}")
        print(f"ECP Only: {summary['ecp_only_count']:,}")
        print()
        
        # Query first 10 results with sorting
        print("=" * 70)
        print("SAMPLE RESULTS (Top 10 by ECP ID, Category, ECC Amount)")
        print("=" * 70)
        print()
        
        results = comparator.query_results(
            job_id=job_id,
            page=1,
            page_size=10,
            sort_by="ecp_id,wage_category,ecc_amount DESC,ecp_amount DESC"
        )
        
        print(f"{'ECP ID':<12} {'Wage Type':<10} {'Category':<15} {'ECC Amt':>15} {'ECP Amt':>15} {'Status':<12}")
        print("-" * 100)
        
        for row in results["results"]:
            ecp_id = row['ecp_id'][:10]
            wt = row['wage_type'][:8]
            cat = (row.get('wage_category') or 'N/A')[:13]
            ecc_amt = f"${row['ecc_amount']:,.2f}"
            ecp_amt = f"${row['ecp_amount']:,.2f}"
            status = row['status']
            
            print(f"{ecp_id:<12} {wt:<10} {cat:<15} {ecc_amt:>15} {ecp_amt:>15} {status:<12}")
        
        print()
        print(f"Pagination: Page {results['pagination']['page']} of {results['pagination']['total_pages']:,}")
        print()
        
        # Test query speed
        print("=" * 70)
        print("TESTING QUERY PERFORMANCE")
        print("=" * 70)
        
        import time
        start = time.time()
        for page in range(1, 6):
            comparator.query_results(job_id=job_id, page=page, page_size=100)
        elapsed = time.time() - start
        
        print(f"✓ Queried 5 pages (500 rows) in {elapsed:.3f} seconds")
        print(f"  Average: {elapsed/5*1000:.1f}ms per page")
        print()
        
        # Clean up
        print("Cleaning up...")
        job_manager.delete_job(job_id)
        print("✓ Job deleted")
        print()
        
    except Exception as e:
        print(f"\n❌ Error: {str(e)}")
        import traceback
        traceback.print_exc()
        return False
    finally:
        comparator.stop_cluster()
    
    return True


if __name__ == "__main__":
    success = test_async_comparison()
    sys.exit(0 if success else 1)
