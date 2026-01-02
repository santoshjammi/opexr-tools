#!/usr/bin/env python3
"""
Convert comparison text files to JSON format for web interface.
Creates an index file and individual JSON files for each employee.
"""

import json
from pathlib import Path
from collections import defaultdict


def parse_amount(amount_str):
    """Parse amount string to float, handling commas and negative values."""
    if not amount_str or amount_str.strip() == '':
        return None
    
    # Remove commas
    amount_str = amount_str.replace(',', '')
    
    # Handle negative values with trailing minus sign
    if amount_str.endswith('-'):
        return -float(amount_str[:-1])
    
    try:
        return float(amount_str)
    except ValueError:
        return None


def parse_comparison_file(file_path):
    """Parse a comparison file and return structured data."""
    with open(file_path, 'r', encoding='utf-8') as f:
        lines = f.readlines()
    
    # Extract header info (line 1)
    header_line = lines[1].strip()
    # Parse "COMPARISON DATA - ECP PERNR: 3274 | ECC PERNR: 37880"
    parts = header_line.split('|')
    ecp_part = parts[0].split(':')[-1].strip()
    ecc_part = parts[1].split(':')[-1].strip()
    
    # Parse data rows (skip first 6 lines: 
    # line 0: separator, line 1: header, line 2: separator, line 3: blank, line 4: column headers, line 5: separator)
    # Data starts at line 6
    data_lines = [line.strip() for line in lines[6:] if line.strip()]
    
    entries = []
    employee_name = None
    
    for line in data_lines:
        parts = line.split('\t')
        if len(parts) < 16:
            continue
        
        if not employee_name:
            employee_name = parts[2]
        
        # Handle missing difference column (when difference is empty/zero)
        difference = parts[16] if len(parts) > 16 else ''
        
        entry = {
            'ecc_pers_no': parts[0],
            'ecp_pers_no': parts[1],
            'name': parts[2],
            'category': parts[3],
            'wt': parts[4],
            'wt_long_text': parts[5],
            'ecc_py_area': parts[6],
            'ecc_for_period': parts[7],
            'ecc_pmt_date': parts[8],
            'ecc_number': parts[9],
            'ecc_amount': parts[10],
            'ecp_py_area': parts[11],
            'ecp_for_period': parts[12],
            'ecp_pmt_date': parts[13],
            'ecp_number': parts[14],
            'ecp_amount': parts[15],
            'difference': difference,
            'ecc_amount_float': parse_amount(parts[10]),
            'ecp_amount_float': parse_amount(parts[15]),
            'difference_float': parse_amount(difference)
        }
        
        entries.append(entry)
    
    return {
        'ecp_pernr': ecp_part,
        'ecc_pernr': ecc_part,
        'name': employee_name,
        'total_entries': len(entries),
        'entries': entries
    }


def create_index(json_dir):
    """Create an index of all employees for quick lookup."""
    json_files = sorted(json_dir.glob('*.json'))
    
    index = []
    for json_file in json_files:
        with open(json_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        index.append({
            'ecp_pernr': data['ecp_pernr'],
            'ecc_pernr': data['ecc_pernr'],
            'name': data['name'],
            'total_entries': data['total_entries'],
            'file': json_file.name
        })
    
    return index


def main():
    """Convert all comparison files to JSON."""
    comparison_dir = Path('data/comparison')
    json_dir = Path('data/json')
    
    # Create JSON directory
    json_dir.mkdir(parents=True, exist_ok=True)
    
    # Get all comparison files
    comparison_files = sorted(comparison_dir.glob('*.txt'))
    
    if not comparison_files:
        print("No comparison files found!")
        return
    
    print(f"Converting {len(comparison_files)} comparison files to JSON...")
    
    processed = 0
    errors = 0
    
    for i, comp_file in enumerate(comparison_files, 1):
        try:
            # Parse the comparison file
            data = parse_comparison_file(comp_file)
            
            # Write JSON file
            json_file = json_dir / f"{comp_file.stem}.json"
            with open(json_file, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2)
            
            processed += 1
            
            if processed % 5000 == 0:
                print(f"  Processed {processed}/{len(comparison_files)} files ({processed*100//len(comparison_files)}%)...")
        
        except Exception as e:
            if errors < 5:
                print(f"Error processing {comp_file.name}: {e}")
            errors += 1
    
    print(f"\nCreating index file...")
    
    # Create index
    index = create_index(json_dir)
    index_file = json_dir / 'index.json'
    
    with open(index_file, 'w', encoding='utf-8') as f:
        json.dump(index, f, indent=2)
    
    print(f"\nJSON conversion complete!")
    print(f"  Files converted: {processed}")
    print(f"  Errors: {errors}")
    print(f"  Index created: {index_file}")
    print(f"  Output directory: {json_dir.absolute()}")


if __name__ == '__main__':
    main()
