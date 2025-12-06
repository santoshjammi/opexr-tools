# DBCompare - Async Dask Comparison with DuckDB

## 🚀 Quick Start

### 1. Start the Backend Server

```bash
cd backend
./start_server.sh
```

The server will start on http://localhost:8000

### 2. Open the UI

Open `index.html` in your browser (Chrome/Firefox recommended)

### 3. Run Comparison

1. Go to **"⚡ Async Dask Comparison"** tab
2. Select **ECC Dataset** (e.g., ECCSEP01)
3. Select **ECP Dataset** (e.g., ECP_1)
4. Click **"⚡ Start Async Comparison"**
5. Watch the progress bar in real-time
6. Results appear automatically when complete

---

## ✨ New Features

### Progressive Loading
- **Instant Response**: Job starts immediately, no waiting
- **Real-time Progress**: See status updates every 2 seconds
- **Background Processing**: Continue working while comparison runs

### DuckDB Storage
- **Fast Queries**: < 100ms per page after initial load
- **Multi-level Sorting**: 
  1. ECP ID (ascending)
  2. Wage Category (ascending)
  3. ECC Amount (descending)
  4. ECP Amount (descending)
- **Type Preservation**: All data types from source files maintained

### Smart Pagination
- Browse millions of rows efficiently
- 50/100/200/500 rows per page
- Instant page switching
- No memory issues

---

## 📊 API Endpoints

### Start Comparison
```bash
POST http://localhost:8000/compare/dask/async/start
{
  "ecc_dataset": "ECCSEP01",
  "ecp_dataset": "ECP_1"
}
```

### Check Progress
```bash
GET http://localhost:8000/compare/dask/async/status/{job_id}
```

### Get Results (Sorted & Paginated)
```bash
GET http://localhost:8000/compare/dask/async/results/{job_id}?page=1&page_size=100&sort_by=ecp_id,wage_category,ecc_amount%20DESC
```

### View All Jobs
```bash
GET http://localhost:8000/compare/dask/async/jobs?limit=50
```

---

## 🗂️ Data Flow

```
1. User clicks "Start Async Comparison"
   ↓
2. API returns job_id immediately
   ↓
3. Backend starts Dask processing
   ├─ Load datasets (ECC + ECP)
   ├─ Map employee IDs
   ├─ Aggregate by (employee, wage type)
   ├─ Merge datasets
   └─ Store results in DuckDB
   ↓
4. UI polls status every 2 seconds
   └─ Shows progress bar
   ↓
5. When complete, load first 100 rows
   └─ Sorted by ECP ID → Category → Amount
   ↓
6. User browses pages instantly
```

---

## 🔧 Technical Architecture

### Backend Stack
- **FastAPI**: Async API endpoints
- **Dask**: Distributed data processing
- **DuckDB**: In-memory analytical database
- **Pandas**: Data manipulation

### Data Storage
- **Job Metadata**: JSON files in `data/jobs/`
- **Comparison Results**: DuckDB files in `data/results/`
- **Source Data**: JSON files in `data/encoded/`

### Performance
- **Initial Response**: < 1 second (returns job_id)
- **Full Comparison**: ~10 minutes for 2.8M rows
- **Query Speed**: < 100ms per 100 rows
- **Memory Usage**: Low (paginated, not in-memory)

---

## 📝 File Structure

```
DBCompare/
├── backend/
│   ├── start_server.sh          # Server startup script
│   ├── main.py                  # FastAPI app
│   ├── job_manager.py           # Job tracking system
│   ├── compare_dask_duckdb.py   # Enhanced comparator
│   ├── api_dask_async.py        # Async API endpoints
│   ├── test_async_comparison.py # Test script
│   └── requirements.txt         # Dependencies
├── index.html                   # Web UI
└── data/
    ├── encoded/                 # Source JSON files
    ├── jobs/                    # Job metadata
    └── results/                 # DuckDB result files
```

---

## 🧪 Testing

Run the test script to verify everything works:

```bash
cd backend
source venv/bin/activate
python test_async_comparison.py
```

Expected output:
- Progress bar showing 0% → 100%
- Summary statistics
- Sample of top 10 results
- Query performance metrics

---

## 🎯 Sorting Details

Results are automatically sorted by:

1. **ECP ID** (ascending) - Groups by employee
2. **Wage Category** (ascending) - Groups by type (Earnings, Deductions, etc.)
3. **ECC Amount** (descending) - Largest amounts first
4. **ECP Amount** (descending) - Then by ECP amount

This makes it easy to:
- Review all data for a specific employee
- See highest value transactions first
- Spot discrepancies quickly

---

## 🔍 Example Results

```
ECP ID       Wage Type  Category   ECC Amount      ECP Amount      Status
───────────────────────────────────────────────────────────────────────────
100001       /3FE       Earnings   $125,000.00     $125,000.00     Matched
100001       M010       Benefits   $12,500.00      $12,500.00      Matched
100001       /580       Deduction  $-1,250.00      $-1,250.00      Matched
100002       /3FE       Earnings   $98,750.00      $0.00           ECC Only
100002       /580       Deduction  $0.00           $-987.50        ECP Only
```

---

## 💡 Tips

1. **Job IDs**: Copy from the progress section to reload later
2. **Page Size**: Use 500 for faster browsing, 50 for detailed review
3. **View All Jobs**: See history of all comparisons
4. **Refresh Results**: Click refresh to ensure latest data

---

## 🆘 Troubleshooting

### Server won't start
- Check if port 8000 is available
- Ensure virtual environment is activated
- Verify all dependencies installed: `pip install -r requirements.txt`

### Comparison fails
- Verify dataset files exist in `data/encoded/`
- Check mapping files in `data/realData/`:
  - `PERNR_ECC_ECP.xlsx`
  - `wagetype_classification.xlsx`

### UI not loading results
- Check browser console for errors
- Verify server is running on port 8000
- Try refreshing the page

---

## 📚 API Documentation

Full interactive API docs available at:
http://localhost:8000/docs

---

## 🎉 What's New vs Old Version

| Feature | Old (Sync) | New (Async) |
|---------|------------|-------------|
| Initial Response | 10+ min | < 1 sec |
| Progress Tracking | None | Real-time |
| Storage | JSON | DuckDB |
| Query Speed | N/A | < 100ms |
| Sorting | Single field | Multi-level |
| Type Preservation | Strings | Full types |
| Memory Usage | High | Low |

---

Enjoy fast, efficient comparisons! 🚀
