#!/usr/bin/env bash
#
# Build the landing site + presentations locally and deploy them to the VPS,
# which serves them statically with nginx on port 80 (http://217.26.28.186/).
#
# This mirrors the GitHub Pages build (.github/workflows/publish.yml) but ships
# to the VPS instead of gh-pages, so the site does not depend on GitHub being
# reachable. Run it from anywhere in the repo:
#
#     bash scripts/deploy_landing_vps.sh
#
# Requirements on this PC: quarto, R (for the site/bulletin/deck charts), a
# Python with jupyter+pandas+matplotlib (for the lesson slides), ssh/scp/tar.
# See docs/vps-landing.md for the one-time VPS setup and the full picture.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

# --- config (override via env) ---------------------------------------------
VPS="${VPS_HOST:-mgimo}"                                   # ssh alias/host
DEST="${VPS_LANDING_DIR:-/home/marcel/mgimo-landing}"      # served by nginx
R_BIN="${R_BIN:-/g/R/R-4.5.1/bin}"                         # R bin dir (msys path)
export QUARTO_PYTHON="${QUARTO_PYTHON:-H:/conda/envs/py312/python.exe}"
export PATH="$R_BIN:$PATH"                                 # so quarto finds R

echo "== [1/6] Render the website (site, bulletin, lessons) =="
quarto render .

echo "== [2/6] Render presentations (shared assets, inline charts) =="
quarto render presentations -P inline_charts:true

echo "== [3/6] Bundle presentations into _site/presentations =="
mkdir -p _site/presentations
cp -r presentations/_site/. _site/presentations/

echo "== [4/6] Pack _site =="
tar -C _site -czf _site_deploy.tar.gz .

echo "== [5/6] Upload to ${VPS}:${DEST} =="
scp -q _site_deploy.tar.gz "${VPS}:/tmp/_landing.tar.gz"

echo "== [6/6] Swap contents on the VPS =="
ssh "${VPS}" "set -e; mkdir -p '${DEST}'; rm -rf '${DEST}'/* '${DEST}'/.[!.]* 2>/dev/null || true; tar -C '${DEST}' -xzf /tmp/_landing.tar.gz; rm -f /tmp/_landing.tar.gz; echo 'top-level:'; ls '${DEST}'"

rm -f _site_deploy.tar.gz
echo
echo "Deployed. Open: https://nts.mgimo.ru/"
