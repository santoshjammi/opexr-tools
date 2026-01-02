#!/usr/bin/env python3
"""
Create comparison files from extracted user data.
Compares ECC and ECP data side-by-side with differences.
"""

import csv
from pathlib import Path
from collections import defaultdict


def parse_amount(amount_str):
    """Parse amount string to float, handling commas and negative values."""
    if not amount_str or amount_str.strip() == '':
        return 0.0
    
    # Remove commas
    amount_str = amount_str.replace(',', '')
    
    # Handle negative values with trailing minus sign (e.g., "2,200.00-")
    if amount_str.endswith('-'):
        return -float(amount_str[:-1])
    
    try:
        return float(amount_str)
    except ValueError:
        return 0.0


def format_amount(amount):
    """Format amount with commas and 2 decimal places."""
    if amount == 0.0:
        return ''
    
    # Format with 2 decimal places
    formatted = f"{abs(amount):,.2f}"
    
    # Add minus sign at the end for negative values
    if amount < 0:
        return formatted + '-'
    
    return formatted


def get_category_sort_key(category):
    """Map category to sort key, putting UNKNOWN at the end."""
    if category == 'UNKNOWN':
        return 'ZZZZ_UNKNOWN'
    return category


def create_comparison_file(actuals_file_path, comparison_file_path):
    """
    Create a comparison file from an actuals file.
    Combines ECC and ECP data side-by-side for each wage type.
    """
    
    # Read the actuals file and parse it
    with open(actuals_file_path, 'r', encoding='utf-8') as f:
        lines = f.readlines()
    
    # Extract header information (first 3 lines)
    header_line = lines[0].strip()
    stats_line = lines[1].strip()
    separator_line = lines[2].strip()
    
    # Skip the column header lines (lines 4-5) and start from data (line 6)
    # Line 4 is the column headers, Line 5 is the separator
    data_lines = [line.strip() for line in lines[6:] if line.strip()]
    
    # Parse data into structured format
    ecc_data = {}  # key: (category, wt, long_text) -> data dict
    ecp_data = {}  # key: (category, wt, long_text) -> data dict
    
    pers_no_ecc = None
    pers_no_ecp = None
    name = None
    
    for line in data_lines:
        parts = line.split('\t')
        if len(parts) < 11:
            continue
        
        system = parts[0]
        pers_no = parts[1]
        person_name = parts[2]
        py_area = parts[3]
        for_period = parts[4]
        pmt_date = parts[5]
        wt = parts[6]
        wt_long_text = parts[7]
        number = parts[8]
        amount = parts[9]
        category = parts[10]
        
        # Store personnel numbers and name
        if system == 'ECC':
            pers_no_ecc = pers_no
        elif system == 'ECP':
            pers_no_ecp = pers_no
        
        if not name:
            name = person_name
        
        key = (category, wt, wt_long_text)
        data = {
            'py_area': py_area,
            'for_period': for_period,
            'pmt_date': pmt_date,
            'number': number,
            'amount': amount
        }
        
        if system == 'ECC':
            ecc_data[key] = data
        elif system == 'ECP':
            ecp_data[key] = data
    
    # Get all unique keys (category, wt, long_text combinations)
    all_keys = set(ecc_data.keys()) | set(ecp_data.keys())
    
    # Sort by category then by WT
    sorted_keys = sorted(all_keys, key=lambda x: (get_category_sort_key(x[0]), x[1]))
    
    # Create comparison rows
    comparison_rows = []
    for key in sorted_keys:
        category, wt, wt_long_text = key
        
        ecc = ecc_data.get(key, {})
        ecp = ecp_data.get(key, {})
        
        # Parse amounts
        ecc_amount = parse_amount(ecc.get('amount', ''))
        ecp_amount = parse_amount(ecp.get('amount', ''))
        
        # Calculate difference (ECC - ECP)
        difference = ecc_amount - ecp_amount
        
        row = {
            'pers_no_ecc': pers_no_ecc or '',
            'pers_no_ecp': pers_no_ecp or '',
            'name': name or '',
            'category': category,
            'wt': wt,
            'wt_long_text': wt_long_text,
            'ecc_py_area': ecc.get('py_area', ''),
            'ecc_for_period': ecc.get('for_period', ''),
            'ecc_pmt_date': ecc.get('pmt_date', ''),
            'ecc_number': ecc.get('number', ''),
            'ecc_amount': format_amount(ecc_amount),
            'ecp_py_area': ecp.get('py_area', ''),
            'ecp_for_period': ecp.get('for_period', ''),
            'ecp_pmt_date': ecp.get('pmt_date', ''),
            'ecp_number': ecp.get('number', ''),
            'ecp_amount': format_amount(ecp_amount),
            'difference': format_amount(difference)
        }
        
        comparison_rows.append(row)
    
    # Write comparison file
    with open(comparison_file_path, 'w', encoding='utf-8') as f:
        # Write header
        f.write('=' * 200 + '\n')
        f.write(f"COMPARISON DATA - ECP PERNR: {pers_no_ecp or 'N/A'} | ECC PERNR: {pers_no_ecc or 'N/A'}\n")
        f.write('=' * 200 + '\n')
        f.write('\n')
        
        # Write column headers
        headers = [
            'ECC Pers. No',
            'ECP Pers. No',
            'Last name First name',
            'Category',
            'WT',
            'Wage Type Long Text',
            'ECC PY Area, FP',
            'ECC For-period',
            'ECC Pmt date',
            'ECC Number',
            'ECC Amount',
            'ECP PY Area, FP',
            'ECP For-period',
            'ECP Pmt date',
            'ECP Number',
            'ECP Amount',
            'Difference'
        ]
        f.write('\t'.join(headers) + '\n')
        f.write('-' * 200 + '\n')
        
        # Write data rows
        for row in comparison_rows:
            line = '\t'.join([
                row['pers_no_ecc'],
                row['pers_no_ecp'],
                row['name'],
                row['category'],
                row['wt'],
                row['wt_long_text'],
                row['ecc_py_area'],
                row['ecc_for_period'],
                row['ecc_pmt_date'],
                row['ecc_number'],
                row['ecc_amount'],
                row['ecp_py_area'],
                row['ecp_for_period'],
                row['ecp_pmt_date'],
                row['ecp_number'],
                row['ecp_amount'],
                row['difference']
            ])
            f.write(line + '\n')


def main():
    """Process all actuals files and create comparison files."""
    
    # Define paths
    actuals_dir = Path('data/actuals')
    comparison_dir = Path('data/comparison')
    
    # Create comparison directory if it doesn't exist
    comparison_dir.mkdir(parents=True, exist_ok=True)
    
    # Get all actuals files
    actuals_files = sorted(actuals_dir.glob('*.txt'))
    
    if not actuals_files:
        print("No actuals files found in data/actuals/")
        return
    
    print(f"Processing {len(actuals_files)} actuals files...")
    
    processed = 0
    errors = 0
    
    for i, actuals_file in enumerate(actuals_files, 1):
        try:
            comparison_file = comparison_dir / actuals_file.name
            create_comparison_file(actuals_file, comparison_file)
            processed += 1
            
            if processed % 5000 == 0:
                print(f"  Processed {processed}/{len(actuals_files)} files ({processed*100//len(actuals_files)}%)...")
        
        except Exception as e:
            if errors < 5:  # Show first 5 errors
                print(f"Error processing {actuals_file.name}: {e}")
            errors += 1
    
    print(f"\nComparison file generation complete!")
    print(f"  Files created: {processed}")
    print(f"  Errors: {errors}")
    print(f"  Output directory: {comparison_dir.absolute()}")


if __name__ == '__main__':
    main()
