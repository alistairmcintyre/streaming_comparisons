#!/bin/bash
# Register Debezium CDC connectors via Kafka Connect REST API.
# Idempotent: PUT (update-or-create) instead of POST.

set -e

CONNECT_URL=${KAFKA_CONNECT_URL:-http://localhost:8083}
CONNECTOR_DIR=${CONNECTOR_DIR:-$(dirname "$0")/../infra/kafka-connect/connectors}

echo "Waiting for Kafka Connect at ${CONNECT_URL}..."
until curl -sf "${CONNECT_URL}/connectors" > /dev/null; do
  echo "  Not ready, retrying in 3s..."
  sleep 3
done
echo "Kafka Connect is ready."

register() {
  local file=$1
  local name
  name=$(python3 -c "import json,sys; print(json.load(open('${file}'))['name'])" 2>/dev/null \
         || grep -o '"name"[[:space:]]*:[[:space:]]*"[^"]*"' "${file}" | head -1 | cut -d'"' -f4)

  echo "Registering connector: ${name} (${file})"

  # Use PUT /connectors/{name}/config to upsert
  local config_json
  config_json=$(python3 -c "import json,sys; d=json.load(open('${file}')); print(json.dumps(d['config']))" 2>/dev/null \
               || cat "${file}" | python3 -c "import json,sys; d=json.load(sys.stdin); print(json.dumps(d['config']))")

  HTTP_STATUS=$(curl -s -o /tmp/connector_response.json -w "%{http_code}" \
    -X PUT "${CONNECT_URL}/connectors/${name}/config" \
    -H "Content-Type: application/json" \
    -d "${config_json}")

  if [ "${HTTP_STATUS}" -ge 200 ] && [ "${HTTP_STATUS}" -lt 300 ]; then
    echo "  OK (HTTP ${HTTP_STATUS})"
  else
    echo "  FAILED (HTTP ${HTTP_STATUS})"
    cat /tmp/connector_response.json
    exit 1
  fi
}

for json_file in "${CONNECTOR_DIR}"/*.json; do
  register "${json_file}"
done

echo ""
echo "Connector status:"
curl -s "${CONNECT_URL}/connectors?expand=status" | python3 -c "
import json, sys
data = json.load(sys.stdin)
for name, info in data.items():
    state = info.get('status', {}).get('connector', {}).get('state', 'UNKNOWN')
    print(f'  {name}: {state}')
" 2>/dev/null || curl -s "${CONNECT_URL}/connectors"
