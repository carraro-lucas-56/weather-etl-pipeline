import pandas as pd
import logging
import json

logger = logging.getLogger(__name__)

#
# Utils
#

def dict_to_json_file(data_dict : dict, file_path : str, indent=4) -> None:
    """
    Converts a Python dictionary to a JSON file.
    """
    try:
        with open(file_path, 'w') as json_file:
            json.dump(data_dict, json_file, indent=indent)
        logger.info(f"Dictionary successfully written to '{file_path}'")
    except IOError as e:
        logger.error(f"Error writing to file '{file_path}': {e}")
    except TypeError as e:
        logger.error(f"Error dusring JSON serialization: {e}")


def ajust_json_for_bigquery(schema_dict: dict) -> list[dict]:
    """
    Convert pandas JSON schema to BigQuery-compatible schema.
    """

    fields = schema_dict["fields"][1:]
    bq_schema = []

    for field in fields:
        name = field["name"]
        pandas_type = field["type"]

        if name == "date":
            bq_type = "TIMESTAMP"   # REQUIRED for partitioning
            mode = "REQUIRED"
        elif pandas_type == "number":
            bq_type = "FLOAT"
            mode = "NULLABLE"
        elif pandas_type == "integer":
            bq_type = "INTEGER"
            mode = "NULLABLE"
        elif pandas_type == "boolean":
            bq_type = "BOOLEAN"
            mode = "NULLABLE"
        else:
            bq_type = "STRING"
            mode = "NULLABLE"

        bq_schema.append({
            "name": name,
            "type": bq_type,
            "mode": mode
        })

    return bq_schema

#
# TRANSFORMATIONS
#

def merge_dfs(df_weatherAPI : pd.DataFrame ,df_openmateo : pd.DataFrame) -> pd.DataFrame:

   # ajusting datetime columns
    df_weatherAPI.rename(columns={'time': 'date'},inplace=True)

    df_weatherAPI['date'] = pd.to_datetime(df_weatherAPI['date'])
    df_openmateo['date'] = pd.to_datetime(df_openmateo['date'])
    
    df_weatherAPI['date'] = df_weatherAPI['date'].dt.tz_localize('America/Sao_Paulo')
    df_openmateo['date'] = df_openmateo['date'].dt.tz_convert('America/Sao_Paulo')

    # combining the two dataframes
    df_merged = pd.merge(df_openmateo,df_weatherAPI,how='inner',on=['date','city'])

    # combining columns with the same features
    column_pairs = {
        ("temp_c", "temperature_2m"): "temperature_c",
        ("feelslike_c", "apparent_temperature"): "apparent_temperature_c",
        ("humidity", "relative_humidity_2m"): "relative_humidity",
        ("precip_mm", "rain"): "precipitation_mm"
    }

    for (col1, col2), new_col in column_pairs.items():
        if col1 in df_merged.columns and col2 in df_merged.columns:
            df_merged[new_col] = df_merged[[col1, col2]].mean(axis=1)

    # drop the old columns
    df_merged = df_merged.drop(columns=[c for pair in column_pairs for c in pair if c in df_merged.columns])

    return df_merged
