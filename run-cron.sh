#!/usr/bin/env bash
set -euo pipefail

# This script writes the crontab from CRON_SCHEDULE and starts cron in foreground.
# It also runs the script once immediately (useful for initial run / container start).

# Load .env if present (docker-compose mounts it)
if [ -f /app/.env ]; then
  set -o allexport
  # shellcheck disable=SC2046
  eval "$(grep -v '^#' /app/.env | xargs -d '\n')"
  set +o allexport
fi

CRON_SCHEDULE="${CRON_SCHEDULE:-0 3 * * *}"
LOG_CMD="/usr/local/bin/python3 /app/fetch_rates.py >> /proc/1/fd/1 2>&1"  # send to container stdout

echo "Using CRON_SCHEDULE: ${CRON_SCHEDULE}"
# write cron file
CRON_FILE="/etc/cron.d/fetchrates"
echo "${CRON_SCHEDULE} ${LOG_CMD}" > "${CRON_FILE}"
chmod 0644 "${CRON_FILE}"
crontab "${CRON_FILE}"

# Ensure data dir exists and permissions ok
: "${DATA_DIR:=/data}"
mkdir -p "${DATA_DIR}"
chown -R "$(id -u):$(id -g)" "${DATA_DIR}" || true

# Run one initial fetch immediately (so container start triggers a run)
echo "Running initial fetch..."
python3 /app/fetch_rates.py || true

# Start cron in foreground (daemon mode if available)
echo "Starting cron..."
# Debian/Ubuntu uses cron; run in foreground by tailing logs after starting
service cron start
# keep container running by tailing syslog (cron writes to syslog). If syslog isn't present, fallback to sleep loop.
if [ -f /var/log/syslog ]; then
  tail -n +1 -F /var/log/syslog
else
  # fallback keepalive
  while true; do sleep 3600; done
fi