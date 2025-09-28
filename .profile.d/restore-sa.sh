#!/usr/bin/env bash
set -e
if [ -n "$GOOGLE_SERVICE_ACCOUNT_JSON_BASE64" ]; then
  mkdir -p /app/config/keys
  echo "$GOOGLE_SERVICE_ACCOUNT_JSON_BASE64" | base64 -d > /app/config/keys/google_service_account.json
  export GOOGLE_APPLICATION_CREDENTIALS=/app/config/keys/google_service_account.json
fi
