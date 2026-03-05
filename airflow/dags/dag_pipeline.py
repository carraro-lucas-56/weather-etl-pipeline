import os
import pendulum
import pandas as pd

from airflow.sdk import dag, task
from airflow.exceptions import AirflowFailException
from dotenv import load_dotenv

import weather_etl as w

load_dotenv()
DATA_ROOT = os.getenv('DATA_ROOT')
EMAIL = os.getenv('EMAIL')

# ----------------------------------------------------------------------
# DAG
# ----------------------------------------------------------------------

@dag(
    schedule="0 23 * * *",
    start_date=pendulum.today("America/Sao_Paulo"),
    catchup=False,
    default_args={
        "email": [f'{EMAIL}'],
        "email_on_failure": True,
        "email_on_retry": False,
    },
    tags=["weather_etl_pipeline"]
)
def weather_pipeline():

    # ------------------------------------------------------------------
    # EXTRACT
    # ------------------------------------------------------------------

    @task(multiple_outputs=True)
    def extract(data_interval_start=None):
        """
        Fetch raw weather data for all cities and save as csv files.
        """
        day = data_interval_start.to_date_string()

        # -------- Load cities -----------------------------------------
        cities_path = f"{DATA_ROOT}/cities.csv"

        try:
            df_cities = pd.read_csv(cities_path)
        except FileNotFoundError:
            # if cities.csv doesn't exist, generate it
            w.get_city_coords()
            df_cities = pd.read_csv(cities_path)
        except Exception as e:
            raise AirflowFailException(f"Failed reading cities file: {e}")

        # -------- Paths for raw output --------------------------------
        raw_weather_path   = f"{DATA_ROOT}/raw/weatherAPI_{day}.csv"
        raw_openmeteo_path = f"{DATA_ROOT}/raw/openmeteo_{day}.csv"

        # -------- Fetch API data --------------------------------------
        df_weatherAPI = w.fetch_all(w.get_weatherAPI_data, df_cities, day)
        df_openmeteo  = w.get_openmeteo_data(day, df_cities)

        # -------- Persist raw data ------------------------------------
        df_weatherAPI.to_csv(raw_weather_path, index=False)
        df_openmeteo.to_csv(raw_openmeteo_path, index=False)

        return {
            "weather_path": raw_weather_path,
            "openmeteo_path": raw_openmeteo_path,
            "day": day
        }

    # ------------------------------------------------------------------
    # TRANSFORM
    # ------------------------------------------------------------------

    @task(multiple_outputs=True)
    def transform(weather_path, openmeteo_path, day):
        """
        Merge, clean and prepare data for BigQuery.
        """

        # load from csv
        df_weatherAPI = pd.read_csv(weather_path)
        df_openmeteo  = pd.read_csv(openmeteo_path)

        df_merged = w.merge_dfs(df_weatherAPI, df_openmeteo)

        merged_path = f"{DATA_ROOT}/Data/{day}.csv"
        schema_path = f"{DATA_ROOT}/table_schema.json"

        # -------- Save merged data ------------------------------------
        df_merged.to_csv(merged_path, index=False)

        # -------- Build BigQuery schema --------------------------------
        schema_dict = pd.io.json.build_table_schema(df_merged)
        schema_dict = w.ajust_json_for_bigquery(schema_dict)

        w.dict_to_json_file(schema_dict, schema_path)

        return {
            "merged_path": merged_path,
            "schema_path": schema_path
        }

    # ------------------------------------------------------------------
    # LOAD
    # ------------------------------------------------------------------

    @task()
    def load(merged_path, schema_path):
        """
        Load processed data into BigQuery.
        """

        table_id = w.create_partitioned_table(schema_path)
        w.load_data_to_bigquery(table_id, merged_path, schema_path)   

    # ------------------------------------------------------------------
    # DAG dependencies
    # ------------------------------------------------------------------

    extracted  = extract()
    transformed = transform(
        extracted["weather_path"],
        extracted["openmeteo_path"],
        extracted["day"]
    )
    load(transformed["merged_path"], transformed["schema_path"])

weather_pipeline()