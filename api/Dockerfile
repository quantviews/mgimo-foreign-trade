FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app ./app
COPY migrations ./migrations
COPY scripts ./scripts

# The DuckDB serving file is mounted at runtime (large, changes on each rebuild),
# not baked into the image. Absolute path bypasses the repo-root resolution.
ENV MGIMO_API_DUCKDB_PATH=/data/unified_trade_data.duckdb

EXPOSE 8000

# 2 workers: each opens its own read-only DuckDB connection + asyncpg pool.
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "2"]
