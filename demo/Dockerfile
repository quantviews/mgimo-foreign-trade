FROM python:3.12-slim

WORKDIR /app

# Build context is the repo root (see docker-compose.yml) so this can also
# pull in shared/trade_tools_shared, used by both this app and mcp-server/.
COPY demo/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY demo/app ./app
COPY shared/trade_tools_shared ./trade_tools_shared

EXPOSE 8000

# One worker is enough for the demo; the agent loop is I/O-bound (LLM + our API).
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000", "--proxy-headers", "--forwarded-allow-ips", "*"]
