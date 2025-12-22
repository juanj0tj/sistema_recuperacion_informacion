#!/usr/bin/env bash
set -euo pipefail

# Ejecutar desde cualquier ruta
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

python3 -m venv .venv
source .venv/bin/activate

python -m pip install -U pip wheel setuptools
python -m pip install -r requirements.txt

python scripts/download_nltk.py

echo "OK: backend instalado."