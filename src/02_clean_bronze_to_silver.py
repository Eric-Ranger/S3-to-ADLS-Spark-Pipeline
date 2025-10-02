import os
from pyspark.sql import functions as F, types as T
from pyspark.sql import DataFrame
from datetime import datetime

from common.spark_utils import create_spark

CUTOFF = os.getenv("CUTOFF_DATE", "1900-01-01")

def safe_dates(df: DataFrame, cutoff_str: str) -> DataFrame:
    """
    For any date/timestamp columns, values < cutoff are cast to string to avoid invalid min dates downstream.
    """
    cutoff = datetime.strptime(cutoff_str, "%Y-%m-%d")
    for field in df.schema.fields:
        if isinstance(field.dataType, (T.DateType, T.TimestampType)):
            colname = field.name
            # Tag rows with ancient dates
            ancient = F.col(colname) < F.lit(cutoff)
            df = df.withColumn(
                colname,
                F.when(ancient, F.col(colname).cast("string")).otherwise(F.col(colname))
            )
    return df

def normalize_names(df: DataFrame) -> DataFrame:
    # Trim/normalize column names
    for c in df.columns:
        new_c = c.strip().replace(" ", "_").replace("-", "_")
        if new_c != c:
            df = df.withColumnRenamed(c, new_c)
    return df

def drop_all_null_columns(df: DataFrame) -> DataFrame:
    to_drop = [c for c in df.columns if df.select(F.count(F.col(c).isNotNull().cast("int"))).first()[0] == 0]
    return df.drop(*to_drop) if to_drop else df

def main():
    spark = create_spark("Clean Bronze → Silver")

    adls_acct  = os.getenv("ADLS_ACCOUNT", "<storage-account>")
    adls_cont  = os.getenv("ADLS_CONTAINER", "<container>")
    bronze_root = os.getenv("BRONZE_PATH", f"abfss://{adls_cont}@{adls_acct}.dfs.core.windows.net/bronze")
    silver_root = os.getenv("SILVER_PATH", f"abfss://{adls_cont}@{adls_acct}.dfs.core.windows.net/silver")

    # Read all subfolders and the latest dt partition for each (simple approach)
    # In production you might pass a specific dt; here we just glob all and rely on overwriteByPath
    # or use "max(dt)" per entity.
    from pyspark.sql import DataFrame

    def process_entity(entity: str):
        src = f"{bronze_root}/{entity}"
        try:
            df = spark.read.option("mergeSchema", "true").parquet(src)
        except Exception as e:
            print(f"No data for {entity}: {e}")
            return

        # If dt exists, keep latest partition
        if "dt" in df.columns:
            max_dt = df.select(F.max("dt")).first()[0]
            df = df.filter(F.col("dt") == max_dt)
            print(f"{entity}: using dt={max_dt}")

        # Basic cleaning
        df = normalize_names(df)
        df = safe_dates(df, CUTOFF)
        df = drop_all_null_columns(df)

        tgt = f"{silver_root}/{entity}"
        (df.write.mode("overwrite").format("parquet").save(tgt))
        print(f"✓ Wrote Silver for {entity}")

    # Infer entities by listing immediate subpaths under bronze_root (requires DBFS/ADLS listing).
    # As a simple approach, let user provide a CSV of entities via env var or scan a fixed list.
    entities = os.getenv("ENTITIES_CSV")
    if entities:
        entities = [e.strip() for e in entities.split(",") if e.strip()]
    else:
        # Fallback: common entity names you'd pass at runtime
        entities = ["entity_a", "entity_b"]

    for e in entities:
        process_entity(e)

if __name__ == "__main__":
    main()