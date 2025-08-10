import logging
import requests
import pandas as pd
import time
from google.cloud import bigquery
from sqlalchemy import create_engine

import config

engine = create_engine(config.DB_URL)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s - %(message)s"
)


def get_big_query_data():
    """
    Get Pokemon data from BigQuery.

    @returns pd.DataFrame: DataFrame with Pokemon data from BigQuery.
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
    Get Pokemon data from PokeAPI.

    @param big_query_data pd.DataFrame: DataFrame containing data from BigQuery.
    @returns pd.DataFrame: DataFrame with Pokemon data from PokeAPI.
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
    Merge BigQuery and API data.

    @param big_query_data pd.DataFrame: DataFrame from BigQuery.
    @param api_data pd.DataFrame: DataFrame from PokeAPI.
    @returns pd.DataFrame: Merged DataFrame.
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
    Save DataFrame to PostgreSQL.

    @param dataframe pd.DataFrame: DataFrame to be saved.
    @param table_name str: Name of the table in PostgreSQL.
    @param if_exists str: Behavior if the table exists (default: "replace").
    @returns None
    """
    try:
        dataframe.to_sql(table_name, engine, if_exists=if_exists, index=False)
        logging.info(f"Successfully saved data to PostgreSQL table '{table_name}'.")

    except Exception as e:
        logging.error(f"Failed to save data to PostgreSQL table '{table_name}': {str(e)}")
        raise


def send_discord_message(content):
    """
    Send a message to a Discord webhook.

    @param content str: Message content to send to Discord.
    """
    if config.CRONTAB and config.DISCORD_WEBHOOK_URL:
        requests.post(config.DISCORD_WEBHOOK_URL, json={"content": content})


if __name__ == "__main__":
    send_discord_message("Starting Cron Pokemon data pipeline...")
    logging.info(f"Starting {'Cron' if config.CRONTAB else ''} Pokemon data pipeline...")

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

        send_discord_message(f"Cron Pokemon data pipeline completed successfully! {len(api_data)} added.")
        logging.info(f"{'Cron' if config.CRONTAB else ''} Pokemon data pipeline completed successfully!")

    except Exception as e:
        send_discord_message(f"Cron Pipeline failed with error: {str(e)}")
        logging.error(f"{'Cron' if config.CRONTAB else ''} Pipeline failed with error: {str(e)}")
