#!/bin/sh
set -e

touch /cron/cron.log

# Write a tiny wrapper so cron has the right env
cat >/cron/run_pipeline.sh <<'EOF'
#!/bin/sh
export MY_PIPELINE_RUN_MODE="${MY_PIPELINE_RUN_MODE:-cron}"
export POSTGRES_HOST="${POSTGRES_HOST:-db}"
export POSTGRES_PORT="${POSTGRES_PORT:-5432}"
export POSTGRES_DB="${POSTGRES_DB:-ranking_pokemon}"
export POSTGRES_USER="${POSTGRES_USER:-postgres}"
export POSTGRES_PASSWORD="${POSTGRES_PASSWORD:-p}"
export GOOGLE_APPLICATION_CREDENTIALS="${GOOGLE_APPLICATION_CREDENTIALS:-/cron/credentials/big_query_credentials.json}"
exec /usr/local/bin/python /cron/pokemon_pipeline.py >> /cron/cron.log 2>&1
EOF
chmod +x /cron/run_pipeline.sh

# Cron job must include the USER field when placed in /etc/cron.d
# TEST: run every minute
# echo "*/1 * * * * root /cron/run_pipeline.sh" > /etc/cron.d/python-cron
# 2 AM
echo "0 2 * * * root /cron/run_pipeline.sh" > /etc/cron.d/python-cron
chmod 0644 /etc/cron.d/python-cron

# Run cron in the foreground so the container stays up
exec /usr/sbin/cron -f

# Keeps the container running
cron &
tail -F /cron/cron.log
