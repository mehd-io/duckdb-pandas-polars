# Install target
install:
	cd $(FOLDER) && poetry install --without dev

# Test target
test:
	cd $(FOLDER) && poetry run pytest

# Run target
run:
	cd $(FOLDER) && poetry run python top_domain.py

download-dataset-sample:
	wget https://duckdb-md-dataset-121.s3.amazonaws.com/hacker_news/hacker_news_2021.zstd.parquet

download-dataset-full:
	wget https://duckdb-md-dataset-121.s3.amazonaws.com/hacker_news/hacker_news_2006_2022.zstd.parquet
