FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY server.py oauth.py ./

# Remote transport. No MGIMO_API_TOKEN here on purpose: each request must carry
# the client's own key in Authorization: Bearer <key> (forwarded to the API).
ENV MCP_TRANSPORT=streamable-http \
    MCP_HOST=0.0.0.0 \
    MCP_PORT=8000 \
    MGIMO_API_BASE=http://trade-api:8000

EXPOSE 8000

CMD ["python", "server.py"]
