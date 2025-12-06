#!/usr/bin/env python3
"""
CLI script to run Dask comparison between ECC and ECP datasets.
"""
import sys
import json
from pathlib import Path
from compare_dask import DaskDataComparator


def main():
    print("=" * 70)
    print("DASK COMPARISON: ECCSEP01 vs ECP_1")
    print("=" * 70)
    print()
    
    # Initialize comparator
    comparator = DaskDataComparator()
    
    try:
        # Run comparison
        print("Starting comparison...")
        print()
        
        result = comparator.compare_datasets_aggregated(
            ecc_dataset="ECCSEP01",
            ecp_dataset="ECP_1",
            page=1,
            page_size=50
        )
        
        print()
        print("=" * 70)
        print("COMPARISON SUMMARY")
        print("=" * 70)
        
        summary = result['summary']
        print(f"Total comparison rows: {summary['total_rows']:,}")
        print(f"Matched rows: {summary['matched_count']:,}")
        print(f"ECC Only: {summary['ecc_only_count']:,}")
        print(f"ECP Only: {summary['ecp_only_count']:,}")
        print()
        print(f"Total ECC Amount: ${summary['total_ecc_amount']:,.2f}")
        print(f"Total ECP Amount: ${summary['total_ecp_amount']:,.2f}")
        print(f"Total Difference: ${summary['total_difference']:,.2f}")
        print()
        
        mappings = result['mappings']
        print(f"Employee mappings used: {mappings['employee_mapping_count']:,}")
        print(f"Wage type mappings used: {mappings['wage_type_mapping_count']:,}")
        print()
        
        print("=" * 70)
        print(f"TOP {len(result['results'])} DIFFERENCES (by absolute value)")
        print("=" * 70)
        print()
        
        # Display results table
        print(f"{'ECP ID':<12} {'WT':<8} {'Category':<20} {'ECC Amt':<15} {'ECP Amt':<15} {'Diff':<15} {'Status':<12}")
        print("-" * 120)
        
        for row in result['results'][:25]:  # Show first 25
            ecp_id = row['ecp_id'][:10]
            wt = row['WT'][:6]
            category = (row.get('wage_category') or 'N/A')[:18]
            ecc_amt = f"${row['ecc_amount']:,.2f}"
            ecp_amt = f"${row['ecp_amount']:,.2f}"
            diff = f"${row['difference']:,.2f}"
            status = row['status']
            
            print(f"{ecp_id:<12} {wt:<8} {category:<20} {ecc_amt:<15} {ecp_amt:<15} {diff:<15} {status:<12}")
        
        print()
        print("=" * 70)
        
        # Save full results to JSON
        output_dir = Path(__file__).parent.parent / "data" / "results"
        output_dir.mkdir(parents=True, exist_ok=True)
        output_file = output_dir / "dask_comparison_ECCSEP01_vs_ECP_1.json"
        
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(result, f, indent=2, default=str)
        
        print(f"Full results saved to: {output_file}")
        print()
        
        # Pagination info
        pagination = result['pagination']
        print(f"Showing page {pagination['page']} of {pagination['total_pages']}")
        print(f"Total rows available: {pagination['total_rows']:,}")
        print()
        
    except Exception as e:
        print(f"\n❌ Error: {str(e)}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        comparator.stop_cluster()
        print("Dask cluster stopped.")


if __name__ == "__main__":
    main()
