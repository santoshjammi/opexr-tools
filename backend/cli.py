#!/usr/bin/env python3
"""
Command-line interface for parsing TXT files to JSON.
"""
import argparse
import sys
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from backend.parser import DaskTxtParser, BatchParser


def parse_single_file(args):
    """Parse a single file."""
    print(f"Parsing file: {args.input}")
    print(f"Delimiter: {repr(args.delimiter)}")
    print(f"Encoding: {args.encoding}")
    
    try:
        parser = DaskTxtParser(
            file_path=args.input,
            delimiter=args.delimiter,
            encoding=args.encoding
        )
        
        result = parser.parse_to_json()
        
        # Save to output file
        parser.save_json(result, args.output, indent=args.indent)
        
        print(f"\n✅ Success!")
        print(f"Rows processed: {result['metadata']['rows']}")
        print(f"Columns: {len(result['metadata']['columns'])}")
        print(f"Output saved to: {args.output}")
        
        if args.show_columns:
            print(f"\nColumns:")
            for col in result['metadata']['columns']:
                dtype = result['metadata']['column_types'].get(col, 'unknown')
                print(f"  - {col}: {dtype}")
        
    except Exception as e:
        print(f"\n❌ Error: {str(e)}")
        sys.exit(1)


def parse_multiple_files(args):
    """Parse multiple files."""
    print(f"Parsing {len(args.inputs)} files...")
    
    batch = BatchParser(delimiter=args.delimiter, encoding=args.encoding)
    results = batch.parse_multiple_files(args.inputs, args.output_dir)
    
    print(f"\n{'=' * 60}")
    print("Results:")
    print(f"{'=' * 60}")
    
    for result in results:
        status_icon = "✅" if result['status'] == 'success' else "❌"
        print(f"{status_icon} {Path(result['file']).name}: {result['status']}")
        if result['status'] == 'success':
            print(f"   Rows: {result['rows']}, Output: {result['output']}")
        else:
            print(f"   Error: {result['error']}")


def main():
    parser = argparse.ArgumentParser(
        description="Parse TXT/CSV files to JSON using Dask",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Parse a tab-separated file
  python cli.py -i data.txt -o output.json

  # Parse with custom delimiter
  python cli.py -i data.csv -o output.json -d ","

  # Parse multiple files
  python cli.py -i file1.txt file2.txt -O results/

  # Show column information
  python cli.py -i data.txt -o output.json --show-columns
        """
    )
    
    parser.add_argument(
        "-i", "--input",
        nargs="+",
        dest="inputs",
        required=True,
        help="Input file(s) to parse"
    )
    
    parser.add_argument(
        "-o", "--output",
        help="Output JSON file (for single file)"
    )
    
    parser.add_argument(
        "-O", "--output-dir",
        help="Output directory (for multiple files)"
    )
    
    parser.add_argument(
        "-d", "--delimiter",
        default="\t",
        help="Field delimiter (default: tab)"
    )
    
    parser.add_argument(
        "-e", "--encoding",
        default="utf-8",
        help="File encoding (default: utf-8)"
    )
    
    parser.add_argument(
        "--indent",
        type=int,
        default=2,
        help="JSON indentation (default: 2)"
    )
    
    parser.add_argument(
        "--show-columns",
        action="store_true",
        help="Display column information"
    )
    
    args = parser.parse_args()
    
    # Validate arguments
    if len(args.inputs) == 1:
        # Single file mode
        if not args.output:
            # Default output name
            input_path = Path(args.inputs[0])
            args.output = str(input_path.with_suffix('.json'))
        
        args.input = args.inputs[0]
        parse_single_file(args)
    else:
        # Multiple files mode
        if not args.output_dir:
            args.output_dir = "results"
        
        parse_multiple_files(args)


if __name__ == "__main__":
    main()
