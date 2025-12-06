#!/usr/bin/env python3
"""
Utility to encode "Last name First name" column in TXT files for testing purposes.
Supports multiple encoding strategies: hash, sequential IDs, or anonymization.
"""
import argparse
import hashlib
import sys
from pathlib import Path
from typing import Dict, Callable


class NameEncoder:
    """Encodes names in tab-separated TXT files."""
    
    def __init__(self, encoding_method: str = "hash"):
        """
        Initialize encoder.
        
        Args:
            encoding_method: 'hash', 'sequential', or 'faker'
        """
        self.encoding_method = encoding_method
        self.name_map: Dict[str, str] = {}
        self.counter = 1
        
    def encode_hash(self, name: str) -> str:
        """Encode name using SHA256 hash (first 8 characters)."""
        if not name or name.strip() == "":
            return name
        hash_obj = hashlib.sha256(name.encode('utf-8'))
        return f"USER_{hash_obj.hexdigest()[:8].upper()}"
    
    def encode_sequential(self, name: str) -> str:
        """Encode name with sequential IDs (USER_0001, USER_0002, etc.)."""
        if not name or name.strip() == "":
            return name
        
        if name not in self.name_map:
            self.name_map[name] = f"USER_{self.counter:04d}"
            self.counter += 1
        return self.name_map[name]
    
    def encode_simple(self, name: str) -> str:
        """Simple encoding: EMPLOYEE_ID format."""
        if not name or name.strip() == "":
            return name
        
        if name not in self.name_map:
            self.name_map[name] = f"EMP_{self.counter:05d}"
            self.counter += 1
        return self.name_map[name]
    
    def get_encoder(self) -> Callable[[str], str]:
        """Get the appropriate encoder function."""
        encoders = {
            'hash': self.encode_hash,
            'sequential': self.encode_sequential,
            'simple': self.encode_simple,
        }
        return encoders.get(self.encoding_method, self.encode_hash)
    
    def process_file(
        self,
        input_path: str,
        output_path: str,
        name_column: str = "Last name First name",
        delimiter: str = "\t",
        encoding: str = "utf-8"
    ):
        """
        Process TXT file and encode the name column.
        
        Args:
            input_path: Input file path
            output_path: Output file path
            name_column: Name of the column to encode
            delimiter: Field delimiter
            encoding: File encoding (default: utf-8)
        """
        input_file = Path(input_path)
        output_file = Path(output_path)
        
        if not input_file.exists():
            raise FileNotFoundError(f"Input file not found: {input_path}")
        
        encoder_func = self.get_encoder()
        
        with open(input_file, 'r', encoding=encoding) as infile:
            # Read header
            header_line = infile.readline()
            columns = [col.strip() for col in header_line.split(delimiter)]
            
            # Find name column index
            try:
                name_col_idx = columns.index(name_column)
            except ValueError:
                raise ValueError(
                    f"Column '{name_column}' not found. "
                    f"Available columns: {', '.join(columns)}"
                )
            
            # Write header to output
            output_file.parent.mkdir(parents=True, exist_ok=True)
            with open(output_file, 'w', encoding=encoding) as outfile:
                outfile.write(header_line)
                
                # Process data rows
                total_rows = 0
                encoded_count = 0
                
                for line in infile:
                    if line.strip():
                        fields = line.split(delimiter)
                        
                        # Encode the name field
                        if name_col_idx < len(fields):
                            original_name = fields[name_col_idx].strip()
                            if original_name:
                                fields[name_col_idx] = encoder_func(original_name)
                                encoded_count += 1
                        
                        outfile.write(delimiter.join(fields))
                        total_rows += 1
                
                print(f"✅ Processing complete!")
                print(f"   Total rows: {total_rows:,}")
                print(f"   Names encoded: {encoded_count:,}")
                print(f"   Unique names: {len(self.name_map):,}")
                print(f"   Output: {output_path}")
    
    def save_mapping(self, mapping_file: str):
        """Save the name mapping to a file for reference."""
        if not self.name_map:
            return
        
        with open(mapping_file, 'w', encoding='utf-8') as f:
            f.write("Original Name\tEncoded Name\n")
            for original, encoded in sorted(self.name_map.items()):
                f.write(f"{original}\t{encoded}\n")
        
        print(f"   Mapping saved: {mapping_file}")


def main():
    parser = argparse.ArgumentParser(
        description="Encode names in TXT files for testing purposes",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Encode with hash method
  python encode_names.py -i ECCSEP05.txt -o ECCSEP05_encoded.txt

  # Encode with sequential IDs
  python encode_names.py -i ECCSEP05.txt -o ECCSEP05_encoded.txt -m sequential

  # Encode and save mapping
  python encode_names.py -i ECCSEP05.txt -o ECCSEP05_encoded.txt -m simple --save-mapping

  # Custom column name
  python encode_names.py -i data.txt -o data_encoded.txt -c "Employee Name"
        """
    )
    
    parser.add_argument(
        "-i", "--input",
        required=True,
        help="Input TXT file path"
    )
    
    parser.add_argument(
        "-o", "--output",
        required=True,
        help="Output TXT file path"
    )
    
    parser.add_argument(
        "-m", "--method",
        choices=['hash', 'sequential', 'simple'],
        default='simple',
        help="Encoding method (default: simple)"
    )
    
    parser.add_argument(
        "-c", "--column",
        default="Last name First name",
        help="Name column to encode (default: 'Last name First name')"
    )
    
    parser.add_argument(
        "-d", "--delimiter",
        default="\t",
        help="Field delimiter (default: tab)"
    )
    
    parser.add_argument(
        "-e", "--encoding",
        default="utf-8",
        help="File encoding (default: utf-8, try utf-16le for some files)"
    )
    
    parser.add_argument(
        "--save-mapping",
        action="store_true",
        help="Save original-to-encoded name mapping"
    )
    
    args = parser.parse_args()
    
    print(f"Encoding names in: {args.input}")
    print(f"Method: {args.method}")
    print(f"Column: {args.column}")
    print()
    
    try:
        encoder = NameEncoder(encoding_method=args.method)
        encoder.process_file(
            input_path=args.input,
            output_path=args.output,
            name_column=args.column,
            delimiter=args.delimiter,
            encoding=args.encoding
        )
        
        if args.save_mapping:
            mapping_file = Path(args.output).with_suffix('.mapping.txt')
            encoder.save_mapping(str(mapping_file))
        
    except Exception as e:
        print(f"\n❌ Error: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main()
