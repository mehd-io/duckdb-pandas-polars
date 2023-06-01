import pandas as pd
import os
from dotenv import load_dotenv
import time
import re

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


def extract_top_domains(df:pd.DataFrame)->pd.DataFrame:
    return (
        df.loc[df["url"].notna()]
        .assign(
            domain=df["url"].apply(
                lambda x: re.findall(r"http[s]?://([^/]+)/", x)[0]
                if x and re.findall(r"http[s]?://([^/]+)/", x)
                else ""
            )
        )
        .query('domain != ""')
        .groupby("domain")
        .size()
        .reset_index(name="count")
        .sort_values("count", ascending=False)
        .head(20)
    )


def main():
    start_time = time.time()
    load_env_vars()

    # load data from local parquet
    root_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__))) 
    df = pd.read_parquet(os.path.join(root_dir, os.getenv("LOCAL_FILE_PATH")))
    
    top_domains = extract_top_domains(df)
    
    # writing to s3
    top_domains.to_parquet(
        f"s3://{os.getenv('S3_BUCKET')}/domain-pandas.parquet",
        storage_options={
            "key": os.getenv("AWS_ACCESS_KEY_ID"),
            "secret": os.getenv("AWS_SECRET_ACCESS_KEY")
        },
    )

    print(f"Pandas execution time: {time.time() - start_time} seconds")


if __name__ == "__main__":
    main()
