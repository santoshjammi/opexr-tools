#!/bin/bash
# Startup script for DBCompare Backend Server

cd "$(dirname "$0")"

echo "=================================================="
echo "  DBCompare Backend Server"
echo "  Starting on http://localhost:8000"
echo "=================================================="
echo ""

# Activate virtual environment
if [ -f "venv/bin/activate" ]; then
    source venv/bin/activate
    echo "✓ Virtual environment activated"
else
    echo "❌ Virtual environment not found!"
    echo "   Run: python3 -m venv venv"
    echo "   Then: source venv/bin/activate"
    echo "   Then: pip install -r requirements.txt"
    exit 1
fi

# Check if dependencies are installed
if ! python -c "import duckdb" 2>/dev/null; then
    echo "⚠️  Installing missing dependencies..."
    pip install -r requirements.txt
fi

echo ""
echo "Starting FastAPI server..."
echo "  • API Docs: http://localhost:8000/docs"
echo "  • UI: Open ../index.html in browser"
echo ""
echo "Press Ctrl+C to stop the server"
echo ""

# Start the server (using backend.main:app since we're in the backend directory)
cd ..
uvicorn backend.main:app --reload --port 8000 --host 0.0.0.0
