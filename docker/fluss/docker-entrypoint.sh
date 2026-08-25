#!/bin/bash
# Entrypoint for our source-built Fluss server (1.0-SNAPSHOT from main, for FIP-27).
# Mirrors the official apache/fluss image entrypoint: append FLUSS_PROPERTIES to
# conf/server.yaml, envsubst it, then map coordinatorServer|tabletServer to the
# dist launch scripts in start-foreground mode.
set -e

CONF_FILE="${FLUSS_HOME}/conf/server.yaml"

prepare_configuration() {
    sed -i '/bind.listeners:/d' "${CONF_FILE}" 2>/dev/null || true
    if [ -n "${FLUSS_PROPERTIES}" ]; then
        echo "${FLUSS_PROPERTIES}" >> "${CONF_FILE}"
    fi
    envsubst < "${CONF_FILE}" > "${CONF_FILE}.tmp" && mv "${CONF_FILE}.tmp" "${CONF_FILE}"
    # Drop config lines whose value rendered empty (e.g. blank S3 keys on AWS IAM)
    # so Fluss falls back to the default credential provider instead of failing.
    sed -i '/^[[:space:]]*[a-zA-Z0-9._-]*:[[:space:]]*$/d' "${CONF_FILE}"
}

prepare_configuration

args=("$@")
if [ "$1" = "help" ]; then
  printf "Usage: docker-entrypoint.sh (coordinatorServer|tabletServer)\n"
  exit 0
elif [ "$1" = "coordinatorServer" ]; then
  args=("${args[@]:1}")
  echo "Starting Coordinator Server"
  exec "$FLUSS_HOME/bin/coordinator-server.sh" start-foreground "${args[@]}"
elif [ "$1" = "tabletServer" ]; then
  args=("${args[@]:1}")
  echo "Starting Tablet Server"
  exec "$FLUSS_HOME/bin/tablet-server.sh" start-foreground "${args[@]}"
fi

exec "${args[@]}"
