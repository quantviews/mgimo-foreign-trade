#!/usr/bin/env bash
set -euo pipefail
# Reuse deployed dependencies; only add the four tested compatibility files.
cd /home/marcel/mgimo-foreign-trade
docker build -t mgimo-trade-api -f - . <<'EOF'
FROM mgimo-trade-api:pre-release-20261024
COPY api/app/comparison.py api/app/comparison_data.py /app/app/
COPY api/app/routers/fizob.py /app/app/routers/
EOF
docker build -t mgimo-trade-demo -f - . <<'EOF'
FROM mgimo-trade-demo:pre-release-20261024
COPY shared/trade_tools_shared/numeric_facts.py /app/trade_tools_shared/
EOF
docker build -t mgimo-trade-mcp -f - . <<'EOF'
FROM mgimo-trade-mcp:pre-release-20261024
COPY shared/trade_tools_shared/numeric_facts.py /app/trade_tools_shared/
EOF
