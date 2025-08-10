import os
import logging
from google.cloud import bigquery

# Google Cloud credentials path
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.path.join(BASE_DIR, "credentials", "big_query_credentials.json")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s - %(message)s"
)


def get_big_query_data():
    """
    Get Pokemon data from BigQuery
    """
    try:
        client = bigquery.Client()
        query = """
            SELECT *
            FROM `bravo-atlas.teste_dev_bravo.ranking_pokemon`
            LIMIT 1000
        """
        df = client.query(query).to_dataframe()
        logging.info(f"Successfully fetched {len(df)} rows from BigQuery.")
        return df
    
    except Exception as e:
        logging.error(f"Failed to fetch data from BigQuery: {str(e)}")
        raise


def get_api_data():
    pass


def merge_results():
    pass


def save_to_postgres():
    pass


if __name__ == "__main__":
    logging.info("Starting Pokemon data pipeline...")
    # Fetch data from BigQuery
    big_query_data = get_big_query_data()
