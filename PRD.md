Standardizing the detailed comparison output is crucial for downstream consumption, such as loading results into a database, feeding a BI tool, or automating alerting.

The current implementation in `data_comparison_etl.py` uses a good approach for finding differences. To make the output *standardized* and *feasible* for a high-volume ETL system (like the FastAPI/Dask setup), we need to formalize the schema into a **Long Format** and ensure the Dask pipeline consistently enforces this schema, regardless of the input file names or column names.

Here is the proposed design approach and the Product Requirements Document (PRD).

### Standardized Comparison Design Approach

The most feasible approach for standardizing the comparison output, especially for very large datasets processed by Dask, is to move from a **Wide Difference Format** (where difference columns are side-by-side) to a **Normalized Long Format** (where each difference is its own row).

#### 1. Standardized Output Schema

We will define a single, fixed schema for all difference reports, allowing all comparison jobs to write to a unified destination (e.g., a central database table or a shared S3 bucket).

| Column Name | Data Type | Description | Example |
| :--- | :--- | :--- | :--- |
| `Job_ID` | String | Unique identifier for the comparison run. | `JOB-A2C4F8` |
| `Comparison_Key` | String | Concatenation of all Primary Key values. (e.g., `MAT_001|PLANT_1000`) | `MAT-123456` |
| `Record_ID_A` | String | Primary Key from File A (Original for traceability) | `MAT-123456` |
| `Record_ID_B` | String | Primary Key from File B (Original for traceability) | `MAT-123456` |
| `Field_Name` | String | The specific value column that differs. | `QUANTITY_CHECK` |
| `Source_Value` | String | The value from the source file (File A). | `100.00` |
| `Target_Value` | String | The value from the target file (File B). | `100.01` |
| `Difference_Type` | String | Categorization of the difference. | `VALUE_MISMATCH`, `MISSING_IN_TARGET` |
| `Report_Timestamp` | Timestamp | When the difference record was created. | `2025-12-01T10:30:00Z` |

#### 2. Dask Implementation Strategy (Standardization)

The Dask ETL process needs to be updated to generate records in this normalized format.

1.  **Missing Records (`A Only` and `B Only`):**
    * For `A Only` records, create rows where:
        * `Field_Name` = `__RECORD_STATUS__`
        * `Source_Value` = `PRESENT`
        * `Target_Value` = `MISSING`
        * `Difference_Type` = `MISSING_IN_TARGET`
    * For `B Only` records, use the same logic but swap `Source_Value` and `Target_Value`, and set `Difference_Type` = `MISSING_IN_SOURCE`.

2.  **Value Differences (`Differences`):**
    * The current logic correctly identifies rows where at least one value column differs.
    * Instead of writing the full row, we use Dask's `melt` or a similar technique (like the manual stacking in the Python script) to loop through only the `value_columns`.
    * For every difference found for a primary key, create one row in the standardized schema:
        * `Field_Name` is the name of the value column (e.g., `QUANTITY_CHECK`).
        * `Source_Value` and `Target_Value` are pulled from the `_A` and `_B` columns.
        * `Difference_Type` = `VALUE_MISMATCH`.

3.  **Key Concatenation:** A helper function will concatenate the `primary_keys` columns (e.g., `MATERIAL_ID` and `DOC_NUMBER`) into the single `Comparison_Key` field using a separator (e.g., `|`) to ensure uniqueness in the output database.



---

### Product Requirements Document (PRD)

#### 1. Introduction

**Document:** Data Migration Validation and Comparison Service PRD
**Product Goal:** To provide a robust, scalable, and standardized data comparison service capable of validating data migration integrity between legacy (File A) and target (File B) systems across large, multi-gigabyte files.
**Audience:** ETL Engineers, Data Analysts, and Migration Project Managers.

#### 2. Goals and Objectives

| ID | Goal | Success Metric |
| :--- | :--- | :--- |
| G1 | **Scalability** | Process 50 million records/file in under 5 minutes on a standard Dask cluster configuration. |
| G2 | **Standardization** | All comparison results must adhere to the standardized Long Format schema defined above. |
| G3 | **Flexibility** | Support CSV and TXT file formats with customizable delimiters. |
| G4 | **Traceability** | Every difference record must be linked back to its originating Job ID and input file paths. |

#### 3. User Stories (Functional Requirements)

| Priority | Role | Need | Feature |
| :--- | :--- | :--- | :--- |
| **High** | ETL Engineer | to configure the primary keys and value columns easily | **API Input Schema:** Use Pydantic models to validate and enforce the presence of required fields (primary keys, value columns, file paths). |
| **High** | Data Analyst | to view all differences in a unified format, regardless of which column mismatched | **Normalized Output:** The Dask pipeline must convert all difference types (Missing and Mismatch) into the Standardized Output Schema. |
| **High** | Migration Manager | to monitor long-running comparison jobs without blocking the UI | **Asynchronous Job Management:** FastAPI must use `BackgroundTasks` to execute the Dask comparison and provide a `/status/{job_id}` endpoint. |
| Medium | ETL Engineer | to handle tab-separated (.txt) files common in SAP exports | **Flexible Delimiters:** Support any single character or the literal `\t` (tab) as a delimiter for both CSV and TXT types. |
| Medium | Data Analyst | to quickly identify records present in only one file | **Difference Typing:** Distinctly categorize output records as `MISSING_IN_SOURCE` or `MISSING_IN_TARGET` in the `Difference_Type` column. |

#### 4. Technical Design Summary

| Component | Technology | Responsibility |
| :--- | :--- | :--- |
| **Frontend** | HTML/Tailwind/JS | Configuration editor, API call triggering, and job status polling. |
| **API Layer** | FastAPI (Python) | Defines the contract (`ComparisonRequest` Pydantic model), receives HTTP requests, and manages job queues. |
| **Processing Engine** | Dask.dataframe (Python) | Handles parallel reading of large, multi-part CSV/TXT files, scalable merging (`ddf_a.merge(ddf_b, how='outer')`), and parallel difference identification and output writing. |
| **Data Flow** | ETL/ELT | **Extract** (Dask read CSV/TXT) -> **Load** (Dask Outer Merge) -> **Transform** (Difference calculation and schema normalization/melting) -> **Load** (Write standardized CSV partitions). |
| **Output** | CSV (Partitioned) | Results written to the `output_directory/{job_id}/` in the standardized Long Format schema. |

#### 5. Architecture & Storage Strategy

Guiding Principles (21st.dev): minimal, composable services; typed contracts; async jobs with observable status; security and versioning by design.

High-Level:

* Tailwind UI (HTML/JS) drives FastAPI.
* FastAPI validates inputs, stages uploads, enqueues Dask jobs.
* Dask workers execute comparisons and emit normalized results.
* Artifacts and metadata are persisted; versioned on disk/object storage.

Database Recommendation:

* PostgreSQL: job metadata, manifests, mapping versions, KPIs, and artifact pointers.
* Redis (optional): job progress cache, pub/sub for UI updates.
* DuckDB (optional): local ad‑hoc analytics on partitioned outputs.
* Object storage: local filesystem for dev; MinIO/S3 in prod. Prefer Parquet for outputs; CSV for interoperability.

#### 6. File Versioning & Folder Conventions

Upload root: `data/`

Folders:

* `data/uploads/{dataset}/{yyyy-mm-dd_HHMMSS}_{job_id}/` — raw upload set.
* `data/mappings/{dataset}/` — versioned mapping files.
* `data/results/{job_id}/` — normalized outputs (CSV/Parquet partitions).
* `data/archive/{dataset}/{version_id}/` — immutable snapshots of prior latest.

Rules:

* Each upload creates a new timestamped `{job_id}` folder under `uploads` and stores A/B (and mapping).
* “Latest” per dataset is the most recent successful job.
* If latest changes: create new folder; retain legacy file and copy a snapshot into the new folder; write `manifest.json` with SHA256 checksums, sizes, roles, mapping version; archive prior latest for rollback.

Manifest (`manifest.json`): `job_id`, `dataset`, `created_at`, `files[]` with `{name,path,sha256,size,role}`, plus `primary_keys[]`, `value_columns[]`, `delimiter`, `mapping_version`.

#### 7. Mapping Files

JSON schema with: `version`, `primary_keys`, `value_columns`, `column_map` (A/B), `type_overrides`, `derived` expressions. Validate via Pydantic. Store at `data/mappings/{dataset}/{version}.json`. Record `mapping_version` in results/manifests.

CSV/Excel Mapping Sheet Support:

* Accept mapping sheets as `.csv` or Excel `.xlsx` uploads with columns:
    * `side` (values: `A` or `B`)
    * `source_column`
    * `canonical_column`
    * optional `dtype` (e.g., `int64`, `float64`, `string`)
    * optional `derived_expr` (for canonical columns computed from expressions)
* FastAPI will parse and normalize the sheet into the JSON mapping schema above.
* Persist the generated JSON mapping under `data/mappings/{dataset}/{version}.json` and store the original sheet alongside it for traceability.
* Include `mapping_source` in `manifest.json` with `{type: csv|xlsx, path, sha256}`.

Example:

```json
{
    "version": "2025-12-02-01",
    "primary_keys": ["MATERIAL_ID", "PLANT"],
    "value_columns": ["QUANTITY", "UOM", "STATUS"],
    "column_map": {
        "A": {"MAT_ID": "MATERIAL_ID", "PLNT": "PLANT"},
        "B": {"Material": "MATERIAL_ID", "PlantCode": "PLANT"}
    },
    "type_overrides": {"QUANTITY": "float64"},
    "derived": [{"name": "QUANTITY_CHECK", "expr": "abs(QUANTITY_A - QUANTITY_B)"}]
}
```

#### 8. API Endpoints (FastAPI)

* `POST /compare` — multipart (A,B,mapping optional) + config; returns `job_id`.
* `GET /status/{job_id}` — queued/running/completed/failed + progress metrics.
* `GET /results/{job_id}` — paths/links to outputs + summary stats.
* `POST /mappings` — create/update mapping versions. Accepts JSON mapping or CSV/XLSX sheet; responds with `mapping_version` and persisted paths.
* `GET /jobs?dataset=...` — list jobs, manifests, pointers.

Security: API key/OAuth, size limits, checksums, MIME validation, encryption at rest (object store), audit logs.

#### 9. Frontend (Tailwind UI/UX)

* Drag‑and‑drop upload for A/B/mapping; config inputs (delimiter, primary keys, value columns).
* Launch job; poll status with progress bar and event logs.
* Results view: KPIs (missing in source/target, mismatches), filterable normalized table, download links.
* Accessibility: semantic HTML, keyboard navigation, contrast compliance.

#### 10. Dask Workflow Details

* Read with explicit dtypes (`type_overrides`); enforce delimiter.
* Apply `column_map` to unify schemas; compute `derived` fields.
* Outer‑merge on `primary_keys`; classify missing‑in‑source/target and mismatches.

* Melt to Long Format; attach `job_id`, `mapping_version`, `comparison_key`.

* Partitioned writes to `data/results/{job_id}/`; record metrics; publish progress via Redis/in‑memory tracker.

#### 11. Non‑Functional Requirements

* Performance: 10M rows < 3 minutes on 4–8 worker cluster (baseline); scale linearly with workers.
* Reliability: atomic manifests; idempotent resumes; retries on transient I/O.

* Observability: structured logs, metrics (rows/sec, partitions), trace IDs per job.

* Compliance: configurable retention; optional PII masking.

#### 12. Deployment & Operations

* Dev: Docker Compose (FastAPI, Postgres, optional Redis, MinIO); filesystem under `data/`.
* Prod: Kubernetes/ECS; S3/MinIO for artifacts; managed Postgres; autoscaled Dask.
* IaC: Terraform/Helm for infra and releases.

#### 13. Open Questions

* File sizes and delimiters for `ECCSEP05.txt`/`ECP_7.txt`?
* Do mappings include joins/reference lookups beyond renames/deriveds?
* Retention and access controls per dataset?
* Need side‑by‑side diff viewer beyond normalized table?
