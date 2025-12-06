"""
Test cases for the TXT parser and FastAPI endpoints.
"""
import pytest
from fastapi.testclient import TestClient
from pathlib import Path
import json
import tempfile

from backend.main import app
from backend.parser import DaskTxtParser
from backend.models import JobStatus


client = TestClient(app)


@pytest.fixture
def sample_txt_file():
    """Create a sample TXT file for testing."""
    content = """Name\tAge\tCity\tSalary
John Doe\t30\tNew York\t75000
Jane Smith\t25\tLos Angeles\t82000
Bob Johnson\t35\tChicago\t68000"""
    
    with tempfile.NamedTemporaryFile(mode='w', suffix='.txt', delete=False) as f:
        f.write(content)
        temp_path = f.name
    
    yield temp_path
    
    # Cleanup
    Path(temp_path).unlink(missing_ok=True)


def test_root_endpoint():
    """Test the root endpoint."""
    response = client.get("/")
    assert response.status_code == 200
    data = response.json()
    assert "service" in data
    assert data["service"] == "Data Comparison API"


def test_parse_endpoint(sample_txt_file):
    """Test file upload and parsing."""
    with open(sample_txt_file, 'rb') as f:
        response = client.post(
            "/parse?delimiter=%09&dataset_name=test",
            files={"file": ("test.txt", f, "text/plain")}
        )
    
    assert response.status_code == 200
    data = response.json()
    assert "job_id" in data
    assert data["status"] == JobStatus.QUEUED
    assert data["dataset_name"] == "test"
    
    return data["job_id"]


def test_status_endpoint_not_found():
    """Test status endpoint with invalid job ID."""
    response = client.get("/status/invalid-job-id")
    assert response.status_code == 404


def test_dask_parser_basic(sample_txt_file):
    """Test basic parsing functionality."""
    parser = DaskTxtParser(
        file_path=sample_txt_file,
        delimiter="\t"
    )
    
    result = parser.parse_to_json()
    
    assert "metadata" in result
    assert "data" in result
    assert result["metadata"]["rows"] == 3
    assert len(result["data"]) == 3
    assert "Name" in result["metadata"]["columns"]


def test_dask_parser_numeric_inference(sample_txt_file):
    """Test numeric column inference."""
    parser = DaskTxtParser(
        file_path=sample_txt_file,
        delimiter="\t"
    )
    
    result = parser.parse_to_json()
    
    # Check that Age and Salary were converted to numeric
    first_record = result["data"][0]
    assert isinstance(first_record["Age"], (int, float))
    assert isinstance(first_record["Salary"], (int, float))


def test_dask_parser_save_json(sample_txt_file):
    """Test saving parsed data to JSON."""
    parser = DaskTxtParser(
        file_path=sample_txt_file,
        delimiter="\t"
    )
    
    result = parser.parse_to_json()
    
    with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
        output_path = f.name
    
    try:
        parser.save_json(result, output_path)
        
        # Verify file was created
        assert Path(output_path).exists()
        
        # Verify content
        with open(output_path, 'r') as f:
            loaded = json.load(f)
            assert loaded["metadata"]["rows"] == 3
    finally:
        Path(output_path).unlink(missing_ok=True)


def test_parser_file_not_found():
    """Test parser with non-existent file."""
    with pytest.raises(FileNotFoundError):
        parser = DaskTxtParser(
            file_path="non_existent_file.txt",
            delimiter="\t"
        )


def test_list_jobs_endpoint():
    """Test listing jobs."""
    response = client.get("/jobs?limit=10")
    assert response.status_code == 200
    data = response.json()
    assert "total" in data
    assert "jobs" in data


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
