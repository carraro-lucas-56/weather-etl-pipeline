import openmeteo_requests
import pandas as pd
import requests_cache
from retry_requests import retry
import requests
from dotenv import load_dotenv
import os
import time
import logging 
from concurrent.futures import ThreadPoolExecutor
from typing import Callable

logger = logging.getLogger(__name__)
load_dotenv()  
    
def get_city_coords():
   
  url = os.getenv('CITIES_URL') 
  df = pd.read_csv(url)

  df = df[['nome','latitude','longitude']].rename(columns={'nome':'city'})
  df = df.head(5000)
  
  df.to_csv("cities.csv", index=False)

def get_openmeteo_data(day : str, df_cities: pd.DataFrame) -> pd.DataFrame:
    """
    Fetch raw data from openmateo api and returns a dataframe contaning only
    the desired featrues.
    """

    CITIES_PER_MINUTE = 500
    CHUNK_SIZE = 50
    SECONDS_PER_MINUTE = 60

    chunks_per_minute = CITIES_PER_MINUTE / CHUNK_SIZE
    seconds_per_chunk = SECONDS_PER_MINUTE / chunks_per_minute 

    # Setup the Open-Meteo API client with cache and retry on error
    cache_session = requests_cache.CachedSession('.cache', expire_after = 3600)
    retry_session = retry(cache_session, retries = 5, backoff_factor = 0.2)
    openmeteo = openmeteo_requests.Client(session = retry_session)

    hourly_features = ["temperature_2m","apparent_temperature", "relative_humidity_2m", "rain"] 

    all_dfs = []

    for i in range(0,len(df_cities),CHUNK_SIZE):

      # getting the right slice from df_cities
      chunk = df_cities.iloc[i:CHUNK_SIZE+i]

      time.sleep(seconds_per_chunk+1)

      url = "https://api.open-meteo.com/v1/forecast"
      params = {
      	"latitude": chunk['latitude'].tolist(),
      	"longitude": chunk['longitude'].tolist(),
      	"hourly": hourly_features,
      	"timezone": "America/Sao_Paulo",
      	"start_date": day,
      	"end_date": day
      }

      try:
        responses = openmeteo.weather_api(url, params=params)
      except Exception as e:
        logger.error(f"Error fetching chunk {i//CHUNK_SIZE}: {e}")
        # responses = openmeteo.weather_api(url, params=params)
        continue

      for index, response in enumerate(responses):

        # Process hourly data. The order of variables needs to be the same as requested.
        hourly = response.Hourly()
        hourly_temperature_2m = hourly.Variables(0).ValuesAsNumpy()
        hourly_apparent_temperature = hourly.Variables(1).ValuesAsNumpy()
        hourly_relative_humidity_2m = hourly.Variables(2).ValuesAsNumpy()
        hourly_rain = hourly.Variables(3).ValuesAsNumpy()

        hourly_data = {"date": pd.date_range(
        	start = pd.to_datetime(hourly.Time(), unit = "s", utc = True).tz_convert('America/Sao_Paulo'),
        	end = pd.to_datetime(hourly.TimeEnd(), unit = "s", utc = True).tz_convert('America/Sao_Paulo'),
        	freq = pd.Timedelta(seconds = hourly.Interval()),
        	inclusive = "left"
        )}

        hourly_data["temperature_2m"] = hourly_temperature_2m
        hourly_data["apparent_temperature"] = hourly_apparent_temperature
        hourly_data["relative_humidity_2m"] = hourly_relative_humidity_2m
        hourly_data["rain"] = hourly_rain

        df = pd.DataFrame(data = hourly_data)
        df['city'] = df_cities['city'].iloc[i+index]
        
        all_dfs.append(df)

    return pd.concat(all_dfs, ignore_index=True)

def get_json(url : str, params : dict) -> dict:
    response = requests.get(url, params)
    return response.json()

def get_weatherAPI_data(day : str, location : str, lat : float, lon : float) -> pd.DataFrame:
    """
    Fetch raw data from weatherAPI and returns a dataframe contaning only
    the desired featrues.
    """

    # TIRAR ISSO DA
    API_KEY = os.getenv("WEATHER_API_KEY")
    BASE_URL_HISTORY = "http://api.weatherapi.com/v1/history.json"
    BASE_URL_CURRENT = "http://api.weatherapi.com/v1/current.json"

    # getting the json containing the air quality features
    try:
      current_json = get_json(BASE_URL_CURRENT,{ "key": API_KEY,
                                                 "q": f'{lat},{lon}',
                                                 "aqi": "yes"
                                               })
      if "error" in current_json:
           logger.error(f"WeatherAPI Error for {location}: {current_json['error']}")
           return pd.DataFrame()

    except Exception as e:
      error_name = e.__class__.__name__
      logger.error(f"{error_name} Error when fetching air quality data for {location} from WeatherAPI")
      return pd.DataFrame()
      
    # getting the json containing the other weather feartures hourly observations
    try:
      hourly_json = get_json(BASE_URL_HISTORY,{  "key": API_KEY,
                                                 "q": f'{lat},{lon}',
                                                 "dt" : day,
                                                 "hourly":"",
                                                 "api": "no"
                                               })
      if "error" in hourly_json:
          logger.error(f"WeatherAPI Error for {location}: {hourly_json['error']}")
          return pd.DataFrame()
      
    except Exception as e:
      error_name = e.__class__.__name__
      logger.error(f"{error_name} Error when fetching air hourly weather data for {location} from WeatherAPI")
      return pd.DataFrame()
      
    # desired features  
    features = ['time', 'temp_c', 'feelslike_c', 'precip_mm', 'humidity',]
    airq_features = [key for key in current_json['current']['air_quality']]

    dict_list = []

    # getting the desired features from the api response body
    for observation in hourly_json['forecast']['forecastday'][0]['hour']:
      dict_temp = {}
      dict_temp['city'] = location

      for key in features:
        dict_temp[key] = observation[key]
      for key in airq_features:
        dict_temp[key] = current_json['current']['air_quality'][key]

      dict_list.append(dict_temp)

    return pd.DataFrame(dict_list)

def fetch_all(func : Callable[[str,str,float,float],pd.DataFrame], 
              df_cities : pd.DataFrame, 
              day : str) -> pd.DataFrame:
    """
    Gets data from multiple cities using threading. 
    """    
    with ThreadPoolExecutor() as executor:    
        futures = [executor.submit(func, day, row.city, row.latitude, row.longitude) for _, row in df_cities.iterrows()]
        results = [f.result() for f in futures]
    return pd.concat(results, ignore_index=True)

