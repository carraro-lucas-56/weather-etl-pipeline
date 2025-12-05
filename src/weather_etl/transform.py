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
        logger.error(f"Error during JSON serialization: {e}")

# ajust the json to the format accepted by google bigquery
def ajust_json_for_bigquery(data : dict) -> None:
    data = data['fields'][1:]
    data[0].pop('tz',None)

    # FIX DATE FIELD TO BE DATETIME

    for item in data:
        if item['type'] == "number":
            item['type'] = "FLOAT"
        else:
            item['type'] = item['type'].upper()
        
    return data


#
# TRANSFORMATIONS
#

def merge_dfs(df_weatherAPI : pd.DataFrame ,df_openmateo : pd.DataFrame) -> pd.DataFrame:

   # ajusting datetime columns
    df_weatherAPI['time'] = pd.to_datetime(df_weatherAPI['time'])
    df_weatherAPI = df_weatherAPI.rename(columns={'time': 'date'})
    df_weatherAPI['date'] = (df_weatherAPI['date'].dt.tz_localize('America/Sao_Paulo'))

    df_openmateo['date'] = pd.to_datetime(df_openmateo['date'])

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

    # removing timezone info
    df_merged["date"] = pd.to_datetime(df_merged["date"]).dt.strftime("%Y-%m-%d %H:%M:%S")

    return df_merged