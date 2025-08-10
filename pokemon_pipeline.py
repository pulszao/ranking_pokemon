import os
import logging
import requests
import pandas as pd
import time
from google.cloud import bigquery
from sqlalchemy import create_engine


discord_webhook_url = "https://discord.com/api/webhooks/your_discord_webhook_url"

CRONTAB = os.getenv("MY_PIPELINE_RUN_MODE", "manual").strip().lower() == "cron"

# Google Cloud credentials path
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
creds_dir = "/app/credentials" if CRONTAB else os.path.join(BASE_DIR, "credentials")
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.path.join(creds_dir, "big_query_credentials.json")

# Database config
PG_CFG = {
    "host": os.getenv("POSTGRES_HOST", "db" if CRONTAB else "localhost"),
    "port": int(os.getenv("POSTGRES_PORT", "5432")),
    "dbname": os.getenv("POSTGRES_DB", "ranking_pokemon"),
    "user": os.getenv("POSTGRES_USER", "postgres"),
    "password": os.getenv("POSTGRES_PASSWORD", "p"),
}
PG_SCHEMA = "public"
DB_URL = f"postgresql+psycopg2://{PG_CFG['user']}:{PG_CFG['password']}@{PG_CFG['host']}:{PG_CFG['port']}/{PG_CFG['dbname']}"
engine = create_engine(DB_URL)

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


def get_api_data(big_query_data):
    """
    Get Pokemon data from PokeAPI
    @param big_query_data: DataFrame containing data from BigQuery
    """
    pokemons_info = []
    total_pokemon = len(big_query_data)
    count = 0
    pokemon_url = f"https://pokeapi.co/api/v2/pokemon"
    species_url = f"https://pokeapi.co/api/v2/pokemon-species"

    try:
        try:
            df_api = pd.read_sql("SELECT numero FROM ranking_pokemon_api", engine)
        except Exception as e:
            # Empty dataframe
            df_api = pd.DataFrame(columns=["numero"])

        
        for _, poke in big_query_data.iterrows():
            # Rate limiting, as maximum API request is 100/s
            time.sleep(0.01)

            if not df_api.empty and poke['numero'] in df_api['numero'].values:
                # logging.debug(f"Skipping Pokemon ID {poke['numero']} as it already exists in the API data.")
                continue
                       
            # Progress logging every 30 Pokemon
            count += 1
            if count % 30 == 0:
                remaining_pokemon = total_pokemon - len(df_api)
                logging.info(f"Processing Pokemon {count}/{remaining_pokemon} from API")

            response = requests.get(f"{pokemon_url}/{poke['numero']}")
            species_response = requests.get(f"{species_url}/{poke['numero']}")

            if response.status_code == 200 and species_response.status_code == 200:
                api_data = response.json()
                species_data = species_response.json()
                pokemons_info.append({
                    'numero': api_data['id'], # pokedex number
                    'tipo': ', '.join(t['type']['name'] for t in api_data['types']),
                    'habilidades': ', '.join(h['ability']['name'] for h in api_data['abilities']),
                    'geracao': species_data['generation']['name'],
                })
                logging.debug(f"Successfully fetched data for Pokemon #{poke['numero']}")
            else:
                logging.warning(f"Failed to fetch data for Pokemon ID {poke['numero']}\n"
                                f"Status codes: (/pokemon/{poke['numero']}): {response.status_code}\n"
                                f"(/pokemon-species/{poke['numero']}): {species_response.status_code}\n")

        logging.info(f"API data fetch completed. {len(pokemons_info)} new Pokemon.")
    
    except Exception as e:
        logging.error(f"Failed to fetch data from PokeAPI: {str(e)}")
        raise
    
    return pd.DataFrame(pokemons_info)


def merge_results(big_query_data, api_data):
    """
    Merge BigQuery and API data
    """
    try:
        merged = pd.merge(big_query_data, api_data, on="numero", suffixes=("_bq", "_api"))
        logging.info(f"Successfully merged data. Result contains {len(merged)} rows.")            
        return merged

    except Exception as e:
        logging.error(f"Failed to merge data: {str(e)}")
        raise


def save_to_postgres(dataframe, table_name, if_exists="replace"):
    """
    Save DataFrame to PostgreSQL
    """
    try:
        dataframe.to_sql(table_name, engine, if_exists=if_exists, index=False)
        logging.info(f"Successfully saved data to PostgreSQL table '{table_name}'.")

    except Exception as e:
        logging.error(f"Failed to save data to PostgreSQL table '{table_name}': {str(e)}")
        raise


if __name__ == "__main__":
    if CRONTAB and discord_webhook_url:
            # Send a message to Discord
        requests.post(discord_webhook_url, json={"content": "Starting Cron Pokemon data pipeline..."})

    logging.info(f"Starting {'Cron' if CRONTAB else ''} Pokemon data pipeline...")

    try:
        # Fetch data from BigQuery
        big_query_data = get_big_query_data()
        save_to_postgres(big_query_data, "ranking_pokemon_bq")
        
        # Fetch data from PokeAPI
        api_data = get_api_data(big_query_data)
        
        # Merge data and save
        if api_data.empty:
            logging.warning("Skipping merge.")
        else:
            save_to_postgres(api_data, "ranking_pokemon_api", if_exists="append")
            merged_data = merge_results(big_query_data, api_data)
            save_to_postgres(merged_data, "ranking_pokemon_merged")
        
        if CRONTAB and discord_webhook_url:
            # Send a message to Discord
            requests.post(discord_webhook_url, json={"content": f"Cron Pokemon data pipeline completed successfully! {len(api_data)} added."})

        logging.info(f"{'Cron' if CRONTAB else ''} Pokemon data pipeline completed successfully!")

    except Exception as e:
        if CRONTAB and discord_webhook_url:
            # Send a message to Discord
            requests.post(discord_webhook_url, json={"content": f"Cron Pipeline failed with error: {str(e)}"})
        logging.error(f"{'Cron' if CRONTAB else ''} Pipeline failed with error: {str(e)}")
