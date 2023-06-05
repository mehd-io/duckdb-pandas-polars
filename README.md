# Duckdb, Pandas & Polars
Code used in the video tutorial [here](https://www.youtube.com/watch?v=4DIoACFItec).
It aims to provides a quick overview of the main differences between the three frameworks through a simple data pipeline.

The pipeline read data from local file, do some transformation and upload to an s3 bucket.

# Development
This repo use python 3.11 & poetry and there's also a [devcontainer](https://code.visualstudio.com/docs/devcontainers/containers) to spin up the environment quickly.

To run each pipeline, you need to provide a `.env` file (located at the root folder) that looks like this : 

```
S3_BUCKET=
LOCAL_FILE_PATH=hacker_news_2006_2022.zstd.parquet
AWS_DEFAULT_REGION=
AWS_SECRET_ACCESS_KEY=
AWS_ACCESS_KEY_ID=
```

The `LOCAL_FILE_PATH` (located in a public S3 Bucket) is to be downloaded first through a make command using `make download-dataset-sample` (for sample of data ~ 600MB) or `make download-dataset-full` for the full dataset (~5GB).

## About dataset
The dataset contains data from [Hacker News](https://news.ycombinator.com/)

## How to run pipelines
Each folder contains its own poetry dependencies definition.

To install these do : `make install FOLDER=x`
where folder name can be `duckdb`,`pandas` or `polars`.

To run the pipeline do : `make run FOLDER=x`
where folder name can be `duckdb`,`pandas` or `polars`
