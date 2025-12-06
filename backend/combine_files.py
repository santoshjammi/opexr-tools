#!/usr/bin/env python3
"""
Combine multiple ECC and ECP files into master files.
Handles UTF-16LE and UTF-8 encodings, preserves headers.
"""

import os
from pathlib import Path
from typing import List, Tuple
import chardet


def detect_encoding(file_path: Path) -> str:
    """Detect file encoding using chardet."""
    with open(file_path, 'rb') as f:
        raw_data = f.read(10000)  # Read first 10KB for detection
        result = chardet.detect(raw_data)
        detected = result['encoding']
        
        # Map common encodings
        if detected and 'UTF-16' in detected.upper():
            return 'utf-16le'
        return 'utf-8'


def read_file_lines(file_path: Path, encoding: str = None) -> Tuple[str, List[str]]:
    """Read file lines with encoding detection."""
    if encoding is None:
        encoding = detect_encoding(file_path)
    
    try:
        with open(file_path, 'r', encoding=encoding, errors='replace') as f:
            lines = f.readlines()
        return encoding, lines
    except Exception as e:
        print(f"Error reading {file_path} with {encoding}: {e}")
        # Try alternative encoding
        alt_encoding = 'utf-8' if encoding == 'utf-16le' else 'utf-16le'
        with open(file_path, 'r', encoding=alt_encoding, errors='replace') as f:
            lines = f.readlines()
        return alt_encoding, lines


def combine_files(file_pattern: str, output_path: Path, source_dir: Path):
    """
    Combine multiple files matching pattern into one master file.
    
    Args:
        file_pattern: Pattern to match (e.g., 'ECC', 'ECP')
        output_path: Output file path
        source_dir: Directory containing source files
    """
    # Find all matching files
    if file_pattern == 'ECC':
        files = sorted([
            f for f in source_dir.glob('ECC*.txt')
            if f.name.startswith('ECC')
        ])
    else:  # ECP
        files = sorted([
            f for f in source_dir.glob('ECP_*.txt')
        ])
    
    if not files:
        print(f"No files found matching pattern: {file_pattern}")
        return
    
    print(f"\n{'='*60}")
    print(f"Combining {len(files)} {file_pattern} files:")
    for f in files:
        print(f"  - {f.name}")
    print(f"{'='*60}\n")
    
    # Read first file to get header
    encoding, first_lines = read_file_lines(files[0])
    header = first_lines[0] if first_lines else ""
    
    print(f"Detected encoding: {encoding}")
    print(f"Header: {header.strip()}")
    
    total_records = 0
    combined_lines = [header]  # Start with header
    
    for i, file_path in enumerate(files):
        print(f"\nProcessing [{i+1}/{len(files)}]: {file_path.name}")
        
        file_encoding, lines = read_file_lines(file_path, encoding)
        
        # Skip header for all files except first
        data_lines = lines[1:] if lines else []
        
        # Filter out empty lines
        data_lines = [line for line in data_lines if line.strip()]
        
        combined_lines.extend(data_lines)
        total_records += len(data_lines)
        
        print(f"  Added {len(data_lines)} records (Total: {total_records})")
    
    # Write combined file
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    # Write as UTF-16LE to maintain consistency with source format
    with open(output_path, 'w', encoding='utf-16le', errors='replace') as f:
        f.writelines(combined_lines)
    
    print(f"\n{'='*60}")
    print(f"✓ Combined file created: {output_path}")
    print(f"  Total records: {total_records:,}")
    print(f"  Total lines (with header): {len(combined_lines):,}")
    print(f"  Output encoding: utf-16le")
    print(f"  File size: {output_path.stat().st_size:,} bytes")
    print(f"{'='*60}\n")


def main():
    """Main function to combine ECC and ECP files."""
    # Set up paths
    base_dir = Path(__file__).parent.parent
    source_dir = base_dir / "data" / "realData"
    output_dir = base_dir / "data" / "combined"
    
    print("\n" + "="*60)
    print("FILE COMBINER - ECC & ECP Master Files")
    print("="*60)
    
    # Combine ECC files
    ecc_output = output_dir / "ECC_MASTER.txt"
    combine_files('ECC', ecc_output, source_dir)
    
    # Combine ECP files
    ecp_output = output_dir / "ECP_MASTER.txt"
    combine_files('ECP', ecp_output, source_dir)
    
    print("\n" + "="*60)
    print("COMBINATION COMPLETE!")
    print("="*60)
    print(f"\nOutput files:")
    print(f"  ECC Master: {ecc_output}")
    print(f"  ECP Master: {ecp_output}")
    print(f"\nNext steps:")
    print(f"  1. Encode both files: python backend/encode_names.py")
    print(f"  2. Parse to JSON: python backend/cli.py")
    print(f"  3. Run comparison: API endpoint /compare")
    print()


if __name__ == "__main__":
    main()
