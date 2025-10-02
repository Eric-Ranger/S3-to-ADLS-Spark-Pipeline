# S3 → ADLS Spark Pipeline (Synapse)

A simple, professional example of a two-step data pipeline that:
1) Ingests the latest partition from an **S3** bucket into **ADLS Bronze**.
2) Cleans and normalizes data from **Bronze** to **Silver** using Spark.

Designed to run in **Azure Synapse** (works locally with Spark, too).

## Structure

```
s3-to-adls-spark-pipeline/
├─ src/
│  ├─ 01_ingest_s3_to_bronze.py
│  ├─ 02_clean_bronze_to_silver.py
│  └─ common/
│     └─ spark_utils.py
├─ conf/
│  ├─ example.env            # optional local env
│  └─ example_config.json    # optional local config
├─ notebooks/
│  └─ synapse_pipeline_demo.ipynb  # placeholder
├─ docs/
│  └─ architecture.md
├─ .vscode/
│  └─ extensions.json
├─ .gitignore
├─ LICENSE
├─ requirements.txt
└─ README.md
```

## Quickstart (Synapse)

- Create a **linked service**/credentials for AWS in Synapse Key Vault and use `mssparkutils.credentials.getSecret(...)` as shown in the code.
- Configure your storage container paths and bucket names in the variables/environment section.
- Execute `01_ingest_s3_to_bronze.py` in a Synapse Spark job/notebook.
- Then execute `02_clean_bronze_to_silver.py`.

## Quickstart (Local Spark)

> For local testing, you can use environment variables or `conf/example.env` instead of Key Vault.
> AWS and ADLS credentials must be provided. Ensure `hadoop-aws` and `azure-storage` connectors are available on your Spark classpath.

```bash
python -m venv .venv
source .venv/bin/activate  # Windows: .venv\Scripts\activate
pip install -r requirements.txt

# Set env vars (or copy conf/example.env to .env and edit)
export AWS_ACCESS_KEY_ID=...
export AWS_SECRET_ACCESS_KEY=...
export S3_BUCKET=my-bucket
export S3_ROOT_PATH=raw/events
export ADLS_CONTAINER=my-container
export ADLS_ACCOUNT=mystorageacct
export BRONZE_PATH=abfss://$ADLS_CONTAINER@$ADLS_ACCOUNT.dfs.core.windows.net/bronze
export SILVER_PATH=abfss://$ADLS_CONTAINER@$ADLS_ACCOUNT.dfs.core.windows.net/silver

# Run
spark-submit src/01_ingest_s3_to_bronze.py
spark-submit src/02_clean_bronze_to_silver.py
```

## Notes

- The code avoids hard-coded secrets and favors **Key Vault** (Synapse) or **environment variables** (local).
- The ingest step picks the **latest date folder** under your `S3_ROOT_PATH` and loads each subfolder as a dataset into **Bronze**.
- The clean step performs schema normalization, safe date casting (handles "ancient" dates), and writes partitioned **Parquet** to **Silver**.

---

Made for showcasing a clean Bronze→Silver process with Spark.