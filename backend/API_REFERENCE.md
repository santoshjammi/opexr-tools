# Data Query API - Quick Reference

## Server Information

**Base URL:** `http://localhost:8000`  
**Interactive Docs:** `http://localhost:8000/docs`  
**Alternative Docs:** `http://localhost:8000/redoc`

## Available Endpoints

### 1. List Available Datasets
Get all available datasets in the encoded directory.

```bash
GET /data/available
```

**Example:**
```bash
curl http://localhost:8000/data/available
```

**Response:**
```json
{
  "datasets": ["ECCSEP05"],
  "count": 1,
  "directory": "/path/to/data/encoded"
}
```

---

### 2. Get Dataset Metadata
Retrieve metadata about a specific dataset.

```bash
GET /data/metadata/{dataset_name}
```

**Example:**
```bash
curl http://localhost:8000/data/metadata/ECCSEP05
```

**Response:**
```json
{
  "dataset": "ECCSEP05",
  "metadata": {
    "source_file": "ECCSEP05.txt",
    "rows": 20434,
    "columns": ["Pers.No.", "Last name First name", ...],
    "column_types": {...},
    "file_size_bytes": 4123346,
    "sha256": "d956a266..."
  }
}
```

---

### 3. Get Dataset Statistics
Get statistical summary of the dataset.

```bash
GET /data/stats/{dataset_name}
```

**Example:**
```bash
curl http://localhost:8000/data/stats/ECCSEP05
```

**Response:**
```json
{
  "dataset": "ECCSEP05",
  "file_info": {
    "source_file": "ECCSEP05.txt",
    "rows": 20434
  },
  "statistics": {
    "unique_employees": 296,
    "unique_wage_types": 107,
    "total_records": 20434,
    "total_amount": 1234567.89,
    "average_amount": 60.42,
    "min_amount": 0.0,
    "max_amount": 75180.54
  }
}
```

---

### 4. Get Records (with Pagination & Filtering)
Retrieve records with pagination and optional filtering.

```bash
GET /data/records/{dataset_name}?skip=0&limit=100&pers_no={number}&emp_id={id}
```

**Query Parameters:**
- `skip` (default: 0) - Number of records to skip
- `limit` (default: 100, max: 1000) - Maximum records to return
- `pers_no` (optional) - Filter by Personnel Number
- `emp_id` (optional) - Filter by Employee ID (e.g., "EMP_00001")

**Examples:**
```bash
# Get first 10 records
curl "http://localhost:8000/data/records/ECCSEP05?skip=0&limit=10"

# Get records for Personnel Number 50169260
curl "http://localhost:8000/data/records/ECCSEP05?pers_no=50169260"

# Get records for Employee EMP_00001
curl "http://localhost:8000/data/records/ECCSEP05?emp_id=EMP_00001"
```

**Response:**
```json
{
  "dataset": "ECCSEP05",
  "total": 20434,
  "skip": 0,
  "limit": 10,
  "count": 10,
  "records": [...]
}
```

---

### 5. Get Employee Records
Get all records for a specific employee with summary.

```bash
GET /data/employee/{dataset_name}/{emp_id}
```

**Example:**
```bash
curl http://localhost:8000/data/employee/ECCSEP05/EMP_00001
```

**Response:**
```json
{
  "dataset": "ECCSEP05",
  "employee_id": "EMP_00001",
  "total_records": 69,
  "total_amount": 1234567.89,
  "wage_types_count": 45,
  "wage_types": ["/101", "/111", "/112", ...],
  "records": [...]
}
```

---

### 6. Search Records
Search and filter records by various criteria.

```bash
GET /data/search/{dataset_name}?wage_type={wt}&period={period}&min_amount={min}&max_amount={max}&limit={limit}
```

**Query Parameters:**
- `wage_type` (optional) - Filter by Wage Type (e.g., "/101")
- `period` (optional) - Filter by For-period (e.g., 202506)
- `min_amount` (optional) - Minimum amount filter
- `max_amount` (optional) - Maximum amount filter
- `limit` (default: 100, max: 1000) - Maximum results

**Examples:**
```bash
# Search by wage type
curl "http://localhost:8000/data/search/ECCSEP05?wage_type=/101&limit=5"

# Search by period
curl "http://localhost:8000/data/search/ECCSEP05?period=202506&limit=10"

# Search by amount range
curl "http://localhost:8000/data/search/ECCSEP05?min_amount=50000&max_amount=60000"

# Combined search
curl "http://localhost:8000/data/search/ECCSEP05?wage_type=/101&period=202506&min_amount=10000"
```

**Response:**
```json
{
  "dataset": "ECCSEP05",
  "filters": {
    "wage_type": "/101",
    "period": 202506,
    "min_amount": 50000,
    "max_amount": 60000
  },
  "total_matches": 25,
  "returned": 25,
  "records": [...]
}
```

---

### 7. Clear Cache
Clear the in-memory data cache.

```bash
DELETE /data/cache
```

**Example:**
```bash
curl -X DELETE http://localhost:8000/data/cache
```

**Response:**
```json
{
  "message": "Cache cleared successfully",
  "items_cleared": 1
}
```

---

## Starting the Server

```bash
cd /Users/kgt/Desktop/Projects/Opexr/DBCompare

# Start server
PYTHONPATH=/Users/kgt/Desktop/Projects/Opexr/DBCompare \
  /Users/kgt/Desktop/Projects/Opexr/DBCompare/backend/venv/bin/python \
  -m uvicorn backend.main:app --reload --port 8000
```

---

## Common Use Cases

### 1. Get Overview of Dataset
```bash
# Get statistics
curl http://localhost:8000/data/stats/ECCSEP05

# Get metadata
curl http://localhost:8000/data/metadata/ECCSEP05
```

### 2. Browse Employee Records
```bash
# Get all records for an employee
curl http://localhost:8000/data/employee/ECCSEP05/EMP_00001

# Get paginated records
curl "http://localhost:8000/data/records/ECCSEP05?emp_id=EMP_00001&skip=0&limit=20"
```

### 3. Analyze Wage Types
```bash
# Get all records with wage type /101
curl "http://localhost:8000/data/search/ECCSEP05?wage_type=/101&limit=100"

# Get statistics to see unique wage types count
curl http://localhost:8000/data/stats/ECCSEP05
```

### 4. Financial Analysis
```bash
# Get high-value transactions
curl "http://localhost:8000/data/search/ECCSEP05?min_amount=50000&limit=50"

# Get records in a specific range
curl "http://localhost:8000/data/search/ECCSEP05?min_amount=10000&max_amount=20000"
```

---

## Python Client Example

```python
import requests

BASE_URL = "http://localhost:8000"

# Get dataset statistics
stats = requests.get(f"{BASE_URL}/data/stats/ECCSEP05").json()
print(f"Total employees: {stats['statistics']['unique_employees']}")

# Get employee records
emp_data = requests.get(f"{BASE_URL}/data/employee/ECCSEP05/EMP_00001").json()
print(f"Employee records: {emp_data['total_records']}")
print(f"Total amount: {emp_data['total_amount']}")

# Search by criteria
results = requests.get(
    f"{BASE_URL}/data/search/ECCSEP05",
    params={
        "wage_type": "/101",
        "min_amount": 50000,
        "limit": 10
    }
).json()
print(f"Found {results['total_matches']} matches")
```

---

## JavaScript/Fetch Example

```javascript
const BASE_URL = "http://localhost:8000";

// Get dataset statistics
fetch(`${BASE_URL}/data/stats/ECCSEP05`)
  .then(res => res.json())
  .then(data => {
    console.log(`Unique employees: ${data.statistics.unique_employees}`);
    console.log(`Total amount: ${data.statistics.total_amount}`);
  });

// Search records
fetch(`${BASE_URL}/data/search/ECCSEP05?wage_type=/101&limit=5`)
  .then(res => res.json())
  .then(data => {
    console.log(`Found ${data.total_matches} matches`);
    data.records.forEach(record => {
      console.log(`${record['Last name First name']}: ${record.Amount}`);
    });
  });
```

---

## Error Responses

### 404 Not Found
```json
{
  "detail": "File not found: data/encoded/UNKNOWN.json"
}
```

### 404 Employee Not Found
```json
{
  "detail": "No records found for employee EMP_99999"
}
```

### 500 Internal Server Error
```json
{
  "detail": "Error loading records: [error message]"
}
```

---

## Performance Notes

- Data is cached in memory after first load for fast subsequent queries
- Use pagination (`skip` and `limit`) for large result sets
- Clear cache with `DELETE /data/cache` if memory usage is a concern
- Maximum limit per request: 1000 records

---

## Testing

Run the test suite:
```bash
cd /Users/kgt/Desktop/Projects/Opexr/DBCompare
/Users/kgt/Desktop/Projects/Opexr/DBCompare/backend/venv/bin/python backend/test_data_api.py
```
