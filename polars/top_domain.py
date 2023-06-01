import polars as pl
from polars import DataFrame, LazyFrame
import os
from dotenv import load_dotenv
import time
import s3fs

REQUIRED_VARS = [
    "AWS_DEFAULT_REGION",
    "AWS_ACCESS_KEY_ID",
    "AWS_SECRET_ACCESS_KEY",
    "LOCAL_FILE_PATH",
    "S3_BUCKET",
]


def load_env_vars(required_vars: list[str] = REQUIRED_VARS):
    load_dotenv()
    for var in required_vars:
        if not os.getenv(var):
            raise Exception(f"Required environment variable not set: {var}")


def extract_top_domains(df: LazyFrame) -> DataFrame:
    return (
        df.filter(pl.col("url").is_not_null())
        .with_columns(pl.col("url").str.extract(r"http[s]?://([^/]+)/").alias("domain"))
        .filter(pl.col("domain") != "")
        .groupby("domain")
        .agg(pl.count("domain").alias("count"))
        .sort("count", descending=True)
        .slice(0, 20)
        .collect()
    )


def main():
    start_time = time.time()
    load_env_vars()

    #df = pl.read_parquet(os.getenv("LOCAL_FILE_PATH"))
    #top_domains = extract_top_domains(df)
    
    root_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__))) 
    df = pl.scan_parquet(os.path.join(root_dir, os.getenv("LOCAL_FILE_PATH")))
    top_domains = extract_top_domains(df)
    
    fs = s3fs.S3FileSystem(
        key=os.getenv("AWS_ACCESS_KEY_ID"), secret=os.getenv("AWS_SECRET_ACCESS_KEY")
    )
    with fs.open(f"{os.getenv('S3_BUCKET')}/domain-polars.parquet", mode="wb") as f:
        top_domains.write_parquet(f)

    print(f"Polars execution time: {time.time() - start_time} seconds")


if __name__ == "__main__":
    main()
