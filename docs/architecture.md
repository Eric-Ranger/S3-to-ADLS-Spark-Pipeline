# Architecture & Flow

**Goal:** Demonstrate a clean, simple S3 → ADLS Bronze → Silver pipeline using Apache Spark.

## Flow

1. **Ingest (01_ingest_s3_to_bronze.py)**
   - Reads AWS credentials from **Key Vault** via `mssparkutils` in Synapse (or from environment variables if running locally).
   - Connects to **S3** with `boto3` (control plane: list folders) and reads Parquet data via **S3A** (`spark.hadoop.fs.s3a.*`).
   - Auto-detects the **latest date folder** under a given root (e.g., `s3://bucket/path/YYYY-MM-DD/`) and iterates subfolders.
   - Writes each dataset into **ADLS Bronze** (Parquet).

2. **Clean (02_clean_bronze_to_silver.py)**
   - Reads Parquet from **Bronze**.
   - Converts problematic "ancient" dates (< 1900-01-01) to strings to preserve data fidelity.
   - Trims/normalizes column names, enforces simple type casting, optionally drops all-null columns.
   - Writes normalized Parquet to **ADLS Silver**.

## Design Choices

- Keep code small and easy to read.
- Minimize dependencies.
- No secrets in code; Key Vault or environment variables only.
- Bronze and Silver remain **schema-on-read friendly** (Parquet).

## Future Ideas

- Add **Delta Lake** for ACID and time travel.
- Add **Quality checks** (Great Expectations / Deequ).
- Add **ADF** or **Synapse pipelines** JSON to orchestrate jobs.