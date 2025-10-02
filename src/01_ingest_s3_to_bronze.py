import os
from datetime import datetime
from botocore.exceptions import ClientError, NoCredentialsError
import boto3
from pyspark.sql import functions as F

from common.spark_utils import create_spark, configure_s3_access

# --- Config: prefer Synapse Key Vault, fallback to env vars ---
def get_secret_or_env(secret_name: str, env_name: str, default: str = None):
    try:
        from notebookutils import mssparkutils  # Synapse runtime
        # Adjust the vault name/secret names to your environment
        vault = os.getenv("SYNAPSE_KEY_VAULT_NAME", "<vault-name>")
        value = mssparkutils.credentials.getSecret(vault, secret_name)
        if value:
            return value
    except Exception:
        pass
    return os.getenv(env_name, default)


def connect_s3_client(access_key: str, secret_key: str, region: str = None):
    session_kwargs = {}
    if region:
        session_kwargs["region_name"] = region
    return boto3.client(
        "s3",
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        **session_kwargs
    )


def list_folders(s3_client, bucket: str, prefix: str):
    # Returns child "folders" under prefix (delimited by '/')
    paginator = s3_client.get_paginator("list_objects_v2")
    result = set()
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix, Delimiter="/"):
        for cp in page.get("CommonPrefixes", []):
            p = cp.get("Prefix", "").rstrip("/")
            if p:
                # return only the last segment
                result.add(p.split("/")[-1])
    return sorted(result)


def infer_latest_date_folder(folders):
    # Expect folders like YYYY-MM-DD or YYYYMMDD; fall back to lexical max
    def parse_dt(s):
        for fmt in ("%Y-%m-%d", "%Y%m%d"):
            try:
                return datetime.strptime(s, fmt)
            except ValueError:
                continue
        return None

    dated = [(f, parse_dt(f)) for f in folders]
    dated = [x for x in dated if x[1] is not None]
    if dated:
        dated.sort(key=lambda x: x[1], reverse=True)
        return dated[0][0]
    # fallback: lexical
    return sorted(folders, reverse=True)[0] if folders else None


def main():
    spark = create_spark("Ingest S3 → ADLS Bronze")

    # Pull configuration
    access_key = get_secret_or_env("<access-key-name>", "AWS_ACCESS_KEY_ID")
    secret_key = get_secret_or_env("<secret-key-name>", "AWS_SECRET_ACCESS_KEY")
    region     = os.getenv("AWS_REGION")

    bucket     = os.getenv("S3_BUCKET", "<bucket-name>")
    root_path  = os.getenv("S3_ROOT_PATH", "<root-path>")  # e.g., path/prefix

    adls_acct  = os.getenv("ADLS_ACCOUNT", "<storage-account>")
    adls_cont  = os.getenv("ADLS_CONTAINER", "<container>")
    bronze_root = os.getenv("BRONZE_PATH", f"abfss://{adls_cont}@{adls_acct}.dfs.core.windows.net/bronze")

    if not (access_key and secret_key):
        raise RuntimeError("Missing AWS credentials. Provide via Synapse Key Vault or env vars.")

    configure_s3_access(spark, access_key, secret_key)

    s3 = connect_s3_client(access_key, secret_key, region)

    # find latest date folder under root
    folders = list_folders(s3, bucket, f"{root_path}/")
    latest = infer_latest_date_folder(folders)
    if not latest:
        print("No date folders found.")
        return
    print(f"Using latest date folder: {latest}")

    # list subfolders under the latest date folder
    subfolders = list_folders(s3, bucket, f"{root_path}/{latest}/")
    if not subfolders:
        print("No subfolders to process.")
        return

    for sub in subfolders:
        s3_path = f"s3a://{bucket}/{root_path}/{latest}/{sub}/"
        target  = f"{bronze_root}/{sub}/dt={latest}"
        print(f"Reading: {s3_path} -> Writing: {target}")
        try:
            df = spark.read.parquet(s3_path)
            # Minimal write to Bronze; keep raw-ish
            (df.write
               .mode("overwrite")
               .format("parquet")
               .save(target))
            print(f"✓ Wrote Bronze for {sub}")
        except Exception as e:
            print(f"✗ Failed {sub}: {e}")


if __name__ == "__main__":
    main()