"""
Centralized configuration module for the Pokemon ranking pipeline.
Stores environment variables, constants, and connection settings.
"""
import os

# Execution mode (manual or cron)
CRONTAB = os.getenv("MY_PIPELINE_RUN_MODE", "manual").strip().lower() == "cron"

# Base directory of the project
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# Google Cloud credentials directory
CREDS_DIR = "/app/credentials" if CRONTAB else os.path.join(BASE_DIR, "credentials")
GOOGLE_APPLICATION_CREDENTIALS = os.path.join(CREDS_DIR, "big_query_credentials.json")
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = GOOGLE_APPLICATION_CREDENTIALS

# PostgreSQL database configuration
PG_CFG = {
    "host": os.getenv("POSTGRES_HOST", "db" if CRONTAB else "localhost"),
    "port": int(os.getenv("POSTGRES_PORT", "5432")),
    "dbname": os.getenv("POSTGRES_DB", "ranking_pokemon"),
    "user": os.getenv("POSTGRES_USER", "postgres"),
    "password": os.getenv("POSTGRES_PASSWORD", "p"),
}
PG_SCHEMA = "public"

# SQLAlchemy connection URL
DB_URL = (
    f"postgresql+psycopg2://{PG_CFG['user']}:{PG_CFG['password']}@"
    f"{PG_CFG['host']}:{PG_CFG['port']}/{PG_CFG['dbname']}"
)

# Discord webhook URL (optional)
DISCORD_WEBHOOK_URL = None  # TODO: Add your Discord webhook URL

# Rate limit for PokeAPI requests (max 100 requests per second)
POKEAPI_RATE_LIMIT_SEC = 0.01
