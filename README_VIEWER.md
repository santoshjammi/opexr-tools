# ECC/ECP Comparison Viewer

A web-based interface to view and compare ECC and ECP payroll data side-by-side.

## Features

- 🔍 **Search**: Find employees by name or PERNR
- 📊 **Side-by-side Comparison**: View ECC and ECP data with calculated differences
- 🎨 **Color Coding**:
  - Green: Matching values
  - Yellow: Differences found
  - Red: ECC only entries
  - Blue: ECP only entries
- 🔎 **Filtering**: Filter by category, wage type, or difference status
- 📋 **Category Badges**: Visual identification of earnings, deductions, taxes, etc.
- 📂 **Tree Structure**: Browse all employees in the left panel
- ⚡ **Lazy Loading**: Efficient loading of large datasets

## Setup

### 1. Convert Comparison Files to JSON

```bash
cd /Users/kgt/Desktop/Projects/Opexr/DBCompare
source backend/venv/bin/activate
python convert_to_json.py
```

This will:
- Read all files from `data/comparison/`
- Convert them to JSON format in `data/json/`
- Create an index file for quick lookup

### 2. Start the API Server

```bash
cd /Users/kgt/Desktop/Projects/Opexr/DBCompare
source backend/venv/bin/activate
uvicorn backend.main:app --reload --port 8000
```

The API will be available at `http://localhost:8000`

### 3. Open the Web Interface

Open your browser and navigate to:
```
http://localhost:8000/viewer
```

This will display the comparison viewer interface.

## API Endpoints

- `GET /api/employees?page=1&per_page=100` - Get list of employees (paginated)
- `GET /api/search?q=<query>&limit=50` - Search employees
- `GET /api/employee/<pernr>` - Get detailed data for an employee
- `GET /api/stats` - Get dataset statistics
- `POST /api/upload` - Upload a comparison file

## File Structure

```
DBCompare/
├── data/
│   ├── comparison/          # Original comparison .txt files
│   ├── json/                # Converted JSON files
│   └── uploads/             # Uploaded files
├── static/
│   └── index.html           # Web interface
├── backend/
│   └── api_comparison.py    # Flask API server
├── convert_to_json.py       # Conversion script
└── README_VIEWER.md         # This file
```

## Usage

1. **Search for an Employee**: Type name or PERNR in the search box
2. **Select an Employee**: Click on an employee in the left panel
3. **View Comparison**: See all wage types with ECC/ECP side-by-side
4. **Apply Filters**: Use the filter dropdowns to focus on specific data
5. **Color Coding**: Rows are color-coded to highlight differences

## Color Legend

- **Green Background**: ECC and ECP values match
- **Yellow Background**: Values differ between ECC and ECP
- **Red Background**: Entry exists only in ECC
- **Blue Background**: Entry exists only in ECP
- **Green Amount**: Positive difference (ECC > ECP)
- **Red Amount**: Negative difference (ECC < ECP)
- **Gray Amount**: No difference or missing data

## Requirements

- Python 3.x
- Flask
- Flask-CORS

Install dependencies:
```bash
pip install flask flask-cors
```

## Notes

- The JSON conversion may take time for large datasets (92,000+ files)
- The conversion runs in the background and creates a log file `json_conversion.log`
- Check the log file for any errors during conversion
