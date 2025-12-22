#!/usr/bin/env bash
set -euo pipefail

BASE="http://localhost:8000"

echo "== / =="
curl -s "$BASE/" | jq .

echo "== /tokenize =="
curl -s -X POST "$BASE/tokenize" \
    -H "Content-Type: application/json" \
    -d '{"document":"Hola mundo. Esto es una prueba de tokenización."}' | jq .