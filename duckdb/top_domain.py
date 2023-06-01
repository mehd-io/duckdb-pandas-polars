import duckdb
from duckdb import DuckDBPyConnection
import os
from dotenv import load_dotenv
import time

REQUIRED_VARS = [
    "AWS_DEFAULT_REGION",
    "AWS_ACCESS_KEY_ID",
    "AWS_SECRET_ACCESS_KEY",
    "LOCAL_FILE_PATH",
    "S3_BUCKET",
]


def load_env_vars(required_vars: list[str] = REQUIRED_VARS):
    # Load .env file
    load_dotenv()
    for var in required_vars:
        if not os.getenv(var):
            raise Exception(f"Required environment variable not set: {var}")


def setup_ddb_connection() -> DuckDBPyConnection:
    # Instantiate the database connection
    conn = duckdb.connect()

    # Install and load HTTPFS extension
    conn.sql("INSTALL httpfs;")
    conn.sql("LOAD httpfs;")

    # Set S3 credentials and region
    conn.sql(f"SET s3_region='{os.getenv('AWS_DEFAULT_REGION')}'")
    conn.sql(f"SET s3_access_key_id='{os.getenv('AWS_ACCESS_KEY_ID')}'")
    conn.sql(f"SET s3_secret_access_key='{os.getenv('AWS_SECRET_ACCESS_KEY')}'")

    return conn


def extract_top_domain(conn: DuckDBPyConnection):
    top_domains = (
        conn.view("hacker_news")
        .filter("url is NOT NULL")
        .project("regexp_extract(url, 'http[s]?://([^/]+)/', 1) AS domain")
        .filter("domain <> ''")
        .aggregate("COUNT(domain) AS count, domain", "domain")
        .order("count DESC")
        .limit(20)
    )
    # Register the result DataFrame as a new table
    conn.register("top_domains", top_domains)


def extract_top_domain_sql(conn: DuckDBPyConnection):
    """ Equivalent of extract_top_domain but using pure SQL"""
    conn.sql(
        """CREATE TABLE top_domains AS 
        SELECT regexp_extract(url, 'http[s]?://([^/]+)/', 1) AS domain,
               COUNT(*) AS count
        FROM hacker_news
        WHERE url IS NOT NULL AND regexp_extract(url, 'http[s]?://([^/]+)/', 1) <> ''
        GROUP BY domain
        ORDER BY count DESC
        LIMIT 20;
        """
    )


def main():
    # Load environment variables
    start_time = time.time()
    load_env_vars()

    # setup ddb connection
    conn = setup_ddb_connection()

    # read data from local file
    root_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    df = conn.read_parquet(os.path.join(root_dir, os.getenv("LOCAL_FILE_PATH")))
    # Create a temporary table from the DataFrame
    conn.register("hacker_news", df)

    # Calculate the top 20 domains
    extract_top_domain(conn)

    conn.sql(
        f"COPY top_domains TO 's3://{os.getenv('S3_BUCKET')}/domain.parquet' (FORMAT 'PARQUET')"
    )

    print(f"DuckDB execution time: {time.time() - start_time} seconds")


if __name__ == "__main__":
    main()
