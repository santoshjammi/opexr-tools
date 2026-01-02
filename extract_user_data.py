#!/usr/bin/env python3
"""
Extract user data from ECC and ECP datasets based on PERNR mapping.

This script:
1. Reads the ECC-ECP PERNR mapping from CSV
2. Finds all ECC entries for each ECC PERNR
3. Finds all ECP entries for the corresponding ECP PERNR
4. Writes combined data to a file named with the ECP PERNR
"""

import csv
import os
import re
from pathlib import Path
from collections import defaultdict

# Configuration
BASE_DIR = Path("/Users/kgt/Desktop/Projects/Opexr/DBCompare/data")
MAPPING_FILE = BASE_DIR / "realData" / "PERNR_ECC_ECP-Sheet1.csv"
WAGETYPE_FILE = BASE_DIR / "realData" / "wagetype_classification-Sheet1.csv"
OUTPUT_DIR = BASE_DIR / "actuals"
DISCREPANCY_DIR = BASE_DIR / "discrepancies"

# ECC and ECP data files
ECC_FILES = [
    "ECCSEP01.txt", "ECCSEP02.txt", "ECCSEP03.txt", "ECCSEP04.txt", 
    "ECCSEP05.txt", "ECCSEPT_2025_IA.txt", "ECCSEPT_2025_IT.txt", "ECCSept_2025_SP.txt"
]
ECP_FILES = ["ECP_1.txt", "ECP_2.txt", "ECP_3.txt", "ECP_4.txt", "ECP_5.txt", "ECP_6.txt", "ECP_7.txt"]

# Column definitions for ECC and ECP
# Note: In ECC, "Last name" and "First name" are combined in field 1
ECC_COLUMNS = {
    'Pers. No': 0,
    'Last name First name': 1,  # Combined field
    'CoCd': 2,
    'PA': 3,
    'PY Area, FP': 4,
    'For-period': 5,
    'Pmt date': 6,
    'WT': 7,
    'Wage Type Long Text': 8,
    'Number': 9,
    'Amount': 10
}

# Note: In ECP, "Last name" and "First name" are also combined in field 1
ECP_COLUMNS = {
    'Pers. No': 0,
    'Last name First name': 1,  # Combined field
    'PY': 2,
    'For-pe': 3,
    'Pmt date': 4,
    'WT': 5,
    'Wage Type Long Text': 6,
    'Number': 7,
    'Amount': 8
}

# Columns to extract (customizable)
EXTRACT_COLUMNS = [
    "Pers. No", "Last name First name", "PY Area, FP", "For-period", 
    "Pmt date", "WT", "Wage Type Long Text", "Number", "Amount"
]


def load_pernr_mapping():
    """Load ECC to ECP PERNR mapping from CSV."""
    mapping = {}
    with open(MAPPING_FILE, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            ecc_pernr = row['ECC PERNR'].strip()
            ecp_pernr = row['ECP PERNR'].strip()
            mapping[ecc_pernr] = ecp_pernr
    print(f"Loaded {len(mapping)} PERNR mappings")
    return mapping


def load_wagetype_classification():
    """Load wage type classification for validation."""
    wagetype_map = {}
    with open(WAGETYPE_FILE, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            wt = row['WT'].strip()
            long_text = row['Wage Type Long Text'].strip()
            category = row['Categories'].strip()
            wagetype_map[wt] = {
                'long_text': long_text,
                'category': category
            }
    print(f"Loaded {len(wagetype_map)} wage type classifications")
    return wagetype_map


def extract_pernr_from_line(line):
    """Extract PERNR (first field) from a data line."""
    if not line or line.startswith('Pers.No.'):
        return None
    
    # PERNR is the first field, separated by whitespace
    parts = line.split(None, 1)
    if parts:
        return parts[0].strip()
    return None


def parse_ecc_line(line, wagetype_map=None):
    """Parse ECC line and extract specified columns with validation."""
    if not line or line.startswith('Pers.No.'):
        return None
    
    # Split by tab delimiter
    parts = [p.strip() for p in line.split('\t')]
    
    # ECC has 11 fields (index 0-10), pad if necessary
    while len(parts) < 11:
        parts.append('')
    
    # Extract only the columns we want
    result = {}
    for col in EXTRACT_COLUMNS:
        if col in ECC_COLUMNS:
            idx = ECC_COLUMNS[col]
            if idx < len(parts):
                result[col] = parts[idx]
            else:
                result[col] = ''
        elif col == 'PY Area, FP' and 'PY Area, FP' in ECC_COLUMNS:
            idx = ECC_COLUMNS['PY Area, FP']
            result[col] = parts[idx] if idx < len(parts) else ''
        else:
            result[col] = ''
    
    # Validate wage type if classification is provided
    if wagetype_map:
        wt = result.get('WT', '').strip()
        long_text = result.get('Wage Type Long Text', '').strip()
        
        if wt in wagetype_map:
            expected_long_text = wagetype_map[wt]['long_text']
            category = wagetype_map[wt]['category']
            result['Category'] = category
            result['Is_Valid'] = (long_text == expected_long_text)
            result['Expected_Long_Text'] = expected_long_text
        else:
            result['Category'] = 'UNKNOWN'
            result['Is_Valid'] = False
            result['Expected_Long_Text'] = 'WT_NOT_FOUND_IN_CLASSIFICATION'
    
    return result


def parse_ecp_line(line, wagetype_map=None):
    """Parse ECP line and extract specified columns with validation."""
    if not line or line.startswith('Pers.No.'):
        return None
    
    # Split by tab delimiter
    parts = [p.strip() for p in line.split('\t')]
    
    # ECP has 9 fields (index 0-8), pad if necessary
    while len(parts) < 9:
        parts.append('')
    
    # Extract only the columns we want, handling column name differences
    result = {}
    for col in EXTRACT_COLUMNS:
        if col == 'PY Area, FP':
            # Map to PY column in ECP
            result[col] = parts[ECP_COLUMNS['PY']] if ECP_COLUMNS['PY'] < len(parts) else ''
        elif col == 'For-period':
            # Map to For-pe column in ECP
            result[col] = parts[ECP_COLUMNS['For-pe']] if ECP_COLUMNS['For-pe'] < len(parts) else ''
        elif col in ECP_COLUMNS:
            idx = ECP_COLUMNS[col]
            if idx < len(parts):
                result[col] = parts[idx]
            else:
                result[col] = ''
        else:
            result[col] = ''
    
    # Validate wage type if classification is provided
    if wagetype_map:
        wt = result.get('WT', '').strip()
        long_text = result.get('Wage Type Long Text', '').strip()
        
        if wt in wagetype_map:
            expected_long_text = wagetype_map[wt]['long_text']
            category = wagetype_map[wt]['category']
            result['Category'] = category
            result['Is_Valid'] = (long_text == expected_long_text)
            result['Expected_Long_Text'] = expected_long_text
        else:
            result['Category'] = 'UNKNOWN'
            result['Is_Valid'] = False
            result['Expected_Long_Text'] = 'WT_NOT_FOUND_IN_CLASSIFICATION'
    
    return result


def process_ecc_files(ecc_pernrs, data_dir, wagetype_map=None):
    """Extract all lines for given ECC PERNRs from all ECC files."""
    ecc_data = defaultdict(list)
    
    for filename in ECC_FILES:
        filepath = data_dir / filename
        if not filepath.exists():
            print(f"Warning: {filename} not found, skipping")
            continue
        
        print(f"Processing {filename}...")
        with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
            header = f.readline()  # Skip header
            
            for line in f:
                pernr = extract_pernr_from_line(line)
                if pernr and pernr in ecc_pernrs:
                    parsed = parse_ecc_line(line, wagetype_map)
                    if parsed:
                        ecc_data[pernr].append(parsed)
    
    return ecc_data


def process_ecp_files(ecp_pernrs, data_dir, wagetype_map=None):
    """Extract all lines for given ECP PERNRs from all ECP files."""
    ecp_data = defaultdict(list)
    
    for filename in ECP_FILES:
        filepath = data_dir / filename
        if not filepath.exists():
            print(f"Warning: {filename} not found, skipping")
            continue
        
        print(f"Processing {filename}...")
        with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
            header = f.readline()  # Skip header
            
            for line in f:
                pernr = extract_pernr_from_line(line)
                if pernr and pernr in ecp_pernrs:
                    parsed = parse_ecp_line(line, wagetype_map)
                    if parsed:
                        ecp_data[pernr].append(parsed)
    
    return ecp_data


def sort_lines_by_category_and_wt(lines):
    """Sort lines by category (UNKNOWN at end) and then by WT."""
    def sort_key(line):
        category = line.get('Category', 'UNKNOWN')
        wt = line.get('WT', '')
        
        # Put UNKNOWN category at the end
        if category == 'UNKNOWN':
            category_order = 'ZZZZ_UNKNOWN'  # Ensures it sorts last
        else:
            category_order = category
        
        return (category_order, wt)
    
    return sorted(lines, key=sort_key)


def write_user_file(ecp_pernr, ecc_pernr, ecc_lines, ecp_lines, output_dir):
    """Write combined ECC and ECP data to a file named with ECP PERNR."""
    output_file = output_dir / f"{ecp_pernr}.txt"
    
    # Sort lines by category and wage type
    ecc_lines_sorted = sort_lines_by_category_and_wt(ecc_lines)
    ecp_lines_sorted = sort_lines_by_category_and_wt(ecp_lines)
    
    # Count valid and invalid entries
    ecc_valid = sum(1 for line in ecc_lines if line.get('Is_Valid', True))
    ecc_invalid = len(ecc_lines) - ecc_valid
    ecp_valid = sum(1 for line in ecp_lines if line.get('Is_Valid', True))
    ecp_invalid = len(ecp_lines) - ecp_valid
    
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write("=" * 180 + "\n")
        f.write(f"COMBINED USER DATA - ECP PERNR: {ecp_pernr} | ECC PERNR: {ecc_pernr}\n")
        f.write(f"ECC Entries: {len(ecc_lines)} (Valid: {ecc_valid}, Invalid: {ecc_invalid}) | ")
        f.write(f"ECP Entries: {len(ecp_lines)} (Valid: {ecp_valid}, Invalid: {ecp_invalid})\n")
        f.write("=" * 180 + "\n\n")
        
        # Write combined header with system suffixes and selected columns plus Category
        header_line = "System\t" + "\t".join(EXTRACT_COLUMNS) + "\tCategory"
        f.write(header_line + "\n")
        f.write("-" * 180 + "\n")
        
        # Write ECC data with ECC prefix (sorted)
        for line_data in ecc_lines_sorted:
            row = ["ECC"] + [line_data.get(col, '') for col in EXTRACT_COLUMNS] + [line_data.get('Category', '')]
            f.write("\t".join(row) + "\n")
        
        # Write ECP data with ECP prefix (sorted)
        for line_data in ecp_lines_sorted:
            row = ["ECP"] + [line_data.get(col, '') for col in EXTRACT_COLUMNS] + [line_data.get('Category', '')]
            f.write("\t".join(row) + "\n")
    
    return output_file


def write_discrepancy_files(ecp_pernr, ecc_pernr, ecc_lines, ecp_lines, discrepancy_dir):
    """Write discrepancy files for entries that don't match wage type classification."""
    ecc_invalid = [line for line in ecc_lines if not line.get('Is_Valid', True)]
    ecp_invalid = [line for line in ecp_lines if not line.get('Is_Valid', True)]
    
    if not ecc_invalid and not ecp_invalid:
        return None
    
    output_file = discrepancy_dir / f"{ecp_pernr}_discrepancies.txt"
    
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write("=" * 200 + "\n")
        f.write(f"WAGE TYPE DISCREPANCIES - ECP PERNR: {ecp_pernr} | ECC PERNR: {ecc_pernr}\n")
        f.write(f"ECC Discrepancies: {len(ecc_invalid)} | ECP Discrepancies: {len(ecp_invalid)}\n")
        f.write("=" * 200 + "\n\n")
        
        if ecc_invalid:
            f.write("--- ECC DISCREPANCIES ---\n")
            header_line = "System\t" + "\t".join(EXTRACT_COLUMNS) + "\tCategory\tExpected Long Text\tIssue"
            f.write(header_line + "\n")
            f.write("-" * 200 + "\n")
            
            for line_data in ecc_invalid:
                actual_long_text = line_data.get('Wage Type Long Text', '')
                expected_long_text = line_data.get('Expected_Long_Text', '')
                issue = "WT not in classification" if expected_long_text == "WT_NOT_FOUND_IN_CLASSIFICATION" else "Long text mismatch"
                
                row = (["ECC"] + 
                       [line_data.get(col, '') for col in EXTRACT_COLUMNS] + 
                       [line_data.get('Category', ''), expected_long_text, issue])
                f.write("\t".join(row) + "\n")
            f.write("\n")
        
        if ecp_invalid:
            f.write("--- ECP DISCREPANCIES ---\n")
            header_line = "System\t" + "\t".join(EXTRACT_COLUMNS) + "\tCategory\tExpected Long Text\tIssue"
            f.write(header_line + "\n")
            f.write("-" * 200 + "\n")
            
            for line_data in ecp_invalid:
                actual_long_text = line_data.get('Wage Type Long Text', '')
                expected_long_text = line_data.get('Expected_Long_Text', '')
                issue = "WT not in classification" if expected_long_text == "WT_NOT_FOUND_IN_CLASSIFICATION" else "Long text mismatch"
                
                row = (["ECP"] + 
                       [line_data.get(col, '') for col in EXTRACT_COLUMNS] + 
                       [line_data.get('Category', ''), expected_long_text, issue])
                f.write("\t".join(row) + "\n")
    
    return output_file


def main():
    # Ensure output directories exist
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    DISCREPANCY_DIR.mkdir(parents=True, exist_ok=True)
    
    # Step 1: Load PERNR mapping
    print("Step 1: Loading PERNR mapping...")
    pernr_mapping = load_pernr_mapping()
    
    # Step 2: Load wage type classification
    print("\nStep 2: Loading wage type classification...")
    wagetype_map = load_wagetype_classification()
    
    # Get all ECC and ECP PERNRs we need to search for
    ecc_pernrs = set(pernr_mapping.keys())
    ecp_pernrs = set(pernr_mapping.values())
    
    data_dir = BASE_DIR / "realData"
    
    # Step 3: Extract all ECC data with validation
    print("\nStep 3: Extracting and validating ECC data...")
    ecc_data = process_ecc_files(ecc_pernrs, data_dir, wagetype_map)
    print(f"Found ECC data for {len(ecc_data)} users")
    
    # Step 4: Extract all ECP data with validation
    print("\nStep 4: Extracting and validating ECP data...")
    ecp_data = process_ecp_files(ecp_pernrs, data_dir, wagetype_map)
    print(f"Found ECP data for {len(ecp_data)} users")
    
    # Step 5: Write combined files and discrepancy files
    print("\nStep 5: Writing output files...")
    files_created = 0
    files_skipped = 0
    discrepancy_files_created = 0
    total_ecc_discrepancies = 0
    total_ecp_discrepancies = 0
    
    for ecc_pernr, ecp_pernr in pernr_mapping.items():
        ecc_lines = ecc_data.get(ecc_pernr, [])
        ecp_lines = ecp_data.get(ecp_pernr, [])
        
        # Only create file if we have data from at least one system
        if ecc_lines or ecp_lines:
            output_file = write_user_file(ecp_pernr, ecc_pernr, ecc_lines, ecp_lines, OUTPUT_DIR)
            files_created += 1
            
            # Write discrepancy file if there are any invalid entries
            discrepancy_file = write_discrepancy_files(ecp_pernr, ecc_pernr, ecc_lines, ecp_lines, DISCREPANCY_DIR)
            if discrepancy_file:
                discrepancy_files_created += 1
                ecc_invalid_count = sum(1 for line in ecc_lines if not line.get('Is_Valid', True))
                ecp_invalid_count = sum(1 for line in ecp_lines if not line.get('Is_Valid', True))
                total_ecc_discrepancies += ecc_invalid_count
                total_ecp_discrepancies += ecp_invalid_count
                
                if discrepancy_files_created <= 5:  # Show first 5 examples
                    print(f"  Discrepancy: {discrepancy_file.name} (ECC: {ecc_invalid_count}, ECP: {ecp_invalid_count})")
            
            if files_created <= 5:  # Show first 5 examples
                print(f"  Created: {output_file.name} (ECC: {len(ecc_lines)}, ECP: {len(ecp_lines)})")
        else:
            files_skipped += 1
    
    print(f"\n{'=' * 80}")
    print(f"SUMMARY:")
    print(f"  Total mappings: {len(pernr_mapping)}")
    print(f"  Files created: {files_created}")
    print(f"  Files skipped (no data): {files_skipped}")
    print(f"  Discrepancy files created: {discrepancy_files_created}")
    print(f"  Total ECC discrepancies: {total_ecc_discrepancies}")
    print(f"  Total ECP discrepancies: {total_ecp_discrepancies}")
    print(f"  Output directory: {OUTPUT_DIR}")
    print(f"  Discrepancy directory: {DISCREPANCY_DIR}")
    print(f"{'=' * 80}")
    
    # Generate comparison files
    print(f"\n{'=' * 80}")
    print("GENERATING COMPARISON FILES...")
    print(f"{'=' * 80}")
    
    try:
        import subprocess
        result = subprocess.run(
            ["python3", "create_comparison.py"],
            capture_output=True,
            text=True,
            cwd=Path(__file__).parent
        )
        
        # Print output from comparison script
        if result.stdout:
            print(result.stdout)
        
        if result.stderr:
            print("Errors:", result.stderr)
        
        if result.returncode != 0:
            print(f"Warning: Comparison script exited with code {result.returncode}")
    
    except Exception as e:
        print(f"Error running comparison script: {e}")


if __name__ == "__main__":
    main()
