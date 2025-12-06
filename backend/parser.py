"""
Dask-based parser for TXT/CSV files with conversion to JSON.
Handles large files efficiently with parallel processing.
"""
import dask.dataframe as dd
import pandas as pd
import json
from pathlib import Path
from typing import Dict, Any, Optional, List
from datetime import datetime
import hashlib


class DaskTxtParser:
    """
    Parser for TXT/CSV files using Dask for scalable processing.
    Converts tabular data to JSON format.
    """
    
    def __init__(
        self,
        file_path: str,
        delimiter: str = "\t",
        encoding: str = "utf-8",
        blocksize: str = "64MB"
    ):
        """
        Initialize the parser.
        
        Args:
            file_path: Path to the TXT/CSV file
            delimiter: Field delimiter (default: tab)
            encoding: File encoding (default: utf-8)
            blocksize: Dask block size for parallel reading
        """
        self.file_path = Path(file_path)
        self.delimiter = delimiter
        self.encoding = encoding
        self.blocksize = blocksize
        
        if not self.file_path.exists():
            raise FileNotFoundError(f"File not found: {file_path}")
    
    def read_file(self) -> dd.DataFrame:
        """
        Read TXT/CSV file using Dask or Pandas (fallback for UTF-16).
        
        Returns:
            Dask DataFrame
        """
        try:
            # UTF-16 files have issues with Dask sampling, use pandas then convert to dask
            if 'utf-16' in self.encoding.lower():
                print("  Using pandas for UTF-16 encoding (then converting to Dask)...")
                pdf = pd.read_csv(
                    str(self.file_path),
                    delimiter=self.delimiter,
                    encoding=self.encoding,
                    dtype=str,
                    on_bad_lines='skip'
                )
                # Clean column names
                pdf.columns = [col.strip() for col in pdf.columns]
                # Convert to Dask DataFrame with partitions
                npartitions = max(1, len(pdf) // 50000)  # ~50k rows per partition
                ddf = dd.from_pandas(pdf, npartitions=npartitions)
                return ddf
            
            # For other encodings, use Dask directly
            sample_size = 256000  # 256KB in bytes
            
            # Read with Dask for parallel processing
            ddf = dd.read_csv(
                str(self.file_path),
                delimiter=self.delimiter,
                encoding=self.encoding,
                blocksize=self.blocksize,
                sample=sample_size,
                dtype=str,  # Read all as strings initially
                assume_missing=True,
                on_bad_lines='skip'
            )
            
            # Clean column names (strip whitespace)
            ddf.columns = [col.strip() for col in ddf.columns]
            
            return ddf
            
        except Exception as e:
            raise ValueError(f"Failed to read file: {str(e)}")
    
    def clean_data(self, ddf: dd.DataFrame) -> dd.DataFrame:
        """
        Clean and normalize data.
        
        Args:
            ddf: Dask DataFrame
            
        Returns:
            Cleaned Dask DataFrame
        """
        # Strip whitespace from all string columns
        for col in ddf.columns:
            ddf[col] = ddf[col].apply(
                lambda x: x.strip() if isinstance(x, str) else x,
                meta=(col, 'object')
            )
        
        return ddf
    
    def infer_numeric_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Attempt to convert numeric-looking columns to appropriate types.
        
        Args:
            df: Pandas DataFrame
            
        Returns:
            DataFrame with converted types
        """
        for col in df.columns:
            # Skip if already numeric
            if pd.api.types.is_numeric_dtype(df[col]):
                continue
            
            # Try to convert to numeric
            try:
                # Remove quotes and commas
                cleaned = df[col].astype(str).str.replace('"', '').str.replace(',', '')
                numeric = pd.to_numeric(cleaned, errors='coerce')
                
                # If most values convert successfully, use numeric type
                if numeric.notna().sum() / len(df) > 0.8:
                    df[col] = numeric
            except:
                pass
        
        return df
    
    def parse_to_json(self) -> Dict[str, Any]:
        """
        Parse the file and convert to JSON structure.
        
        Returns:
            Dictionary with metadata and data records
        """
        # Read file with Dask
        ddf = self.read_file()
        
        # Clean data
        ddf = self.clean_data(ddf)
        
        # Compute to Pandas for final processing
        # For very large files, consider partitioned processing
        df = ddf.compute()
        
        # Infer and convert numeric columns
        df = self.infer_numeric_columns(df)
        
        # Replace NaN with None for JSON compatibility
        df = df.where(pd.notna(df), None)
        
        # Convert to records
        records = df.to_dict(orient='records')
        
        # Calculate file hash for traceability
        file_hash = self._calculate_file_hash()
        
        # Build result
        result = {
            "metadata": {
                "source_file": self.file_path.name,
                "source_path": str(self.file_path),
                "delimiter": self.delimiter,
                "encoding": self.encoding,
                "rows": len(df),
                "columns": list(df.columns),
                "column_types": {col: str(dtype) for col, dtype in df.dtypes.items()},
                "parsed_at": datetime.now().isoformat(),
                "file_size_bytes": self.file_path.stat().st_size,
                "sha256": file_hash
            },
            "data": records
        }
        
        return result
    
    def save_json(self, data: Dict[str, Any], output_path: str, indent: int = 2):
        """
        Save parsed data to JSON file.
        
        Args:
            data: Dictionary to save
            output_path: Output file path
            indent: JSON indentation (default: 2)
        """
        output_file = Path(output_path)
        output_file.parent.mkdir(parents=True, exist_ok=True)
        
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=indent, ensure_ascii=False)
    
    def _calculate_file_hash(self) -> str:
        """Calculate SHA256 hash of the source file."""
        sha256_hash = hashlib.sha256()
        with open(self.file_path, "rb") as f:
            for byte_block in iter(lambda: f.read(4096), b""):
                sha256_hash.update(byte_block)
        return sha256_hash.hexdigest()
    
    @staticmethod
    def parse_file_to_json(
        file_path: str,
        output_path: str,
        delimiter: str = "\t",
        encoding: str = "utf-8"
    ) -> Dict[str, Any]:
        """
        Convenience method to parse a file and save as JSON in one call.
        
        Args:
            file_path: Input file path
            output_path: Output JSON file path
            delimiter: Field delimiter
            encoding: File encoding
            
        Returns:
            Parsed data dictionary
        """
        parser = DaskTxtParser(file_path, delimiter, encoding)
        result = parser.parse_to_json()
        parser.save_json(result, output_path)
        return result


class BatchParser:
    """
    Batch parser for processing multiple files.
    """
    
    def __init__(self, delimiter: str = "\t", encoding: str = "utf-8"):
        self.delimiter = delimiter
        self.encoding = encoding
    
    def parse_multiple_files(
        self,
        file_paths: List[str],
        output_dir: str
    ) -> List[Dict[str, Any]]:
        """
        Parse multiple files in batch.
        
        Args:
            file_paths: List of input file paths
            output_dir: Output directory for JSON files
            
        Returns:
            List of results for each file
        """
        results = []
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)
        
        for file_path in file_paths:
            try:
                parser = DaskTxtParser(file_path, self.delimiter, self.encoding)
                result = parser.parse_to_json()
                
                # Save to output directory
                file_name = Path(file_path).stem
                output_file = output_path / f"{file_name}.json"
                parser.save_json(result, str(output_file))
                
                results.append({
                    "file": file_path,
                    "status": "success",
                    "output": str(output_file),
                    "rows": result["metadata"]["rows"]
                })
            except Exception as e:
                results.append({
                    "file": file_path,
                    "status": "error",
                    "error": str(e)
                })
        
        return results
