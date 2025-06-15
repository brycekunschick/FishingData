import requests
import json
import pandas as pd
from datetime import datetime, timedelta, timezone
import pytz
import time

def get_timezone_object(timezone_abbr):
    """
    Map weather data to timezone
    """
    if timezone_abbr == 'ET':
        return pytz.timezone('America/New_York')
    elif timezone_abbr == 'MT':
        return pytz.timezone('America/Denver')
    elif timezone_abbr == 'CT':
        return pytz.timezone('America/Chicago')
    else:
        # Default to UTC or raise an error if an unknown timezone is encountered
        print(f"Warning: Unknown timezone abbreviation '{timezone_abbr}'")
        return pytz.utc
    
def generate_date_chunks(start_date_str, end_date_str, chunk_size_days):
    """
    Get date values for api in 30 day chunks (request limit)
    """
    start_date = datetime.strptime(start_date_str, '%Y-%m-%d')
    end_date = datetime.strptime(end_date_str, '%Y-%m-%d')

    date_chunks = []
    current_chunk_start = start_date

    while current_chunk_start <= end_date:
        current_chunk_end = current_chunk_start + timedelta(days=chunk_size_days - 1)
        if current_chunk_end > end_date:
            current_chunk_end = end_date

        date_chunks.append({
            'start_date': current_chunk_start.strftime('%Y%m%d'),
            'end_date': current_chunk_end.strftime('%Y%m%d')
        })
        current_chunk_start = current_chunk_end + timedelta(days=1)
    return date_chunks

def fetch_weather_data_for_range(base_url_for_location, start_date_param, end_date_param):
    """
    Fetches historical weather data for date range by appending date values to base URL
    """
    full_url = f"{base_url_for_location}startDate={start_date_param}&endDate={end_date_param}"

    print(f"Fetching: {full_url}")

    try:
        response = requests.get(full_url)
        response.raise_for_status() # Raise an exception for HTTP errors (4xx or 5xx)
        return response.json()
    except requests.exceptions.HTTPError as http_err:
        print(f"HTTP error for {full_url}: {http_err}")
    except requests.exceptions.ConnectionError as conn_err:
        print(f"Connection error for {full_url}: {conn_err}")
    except requests.exceptions.Timeout as timeout_err:
        print(f"Timeout error for {full_url}: {timeout_err}")
    except requests.exceptions.RequestException as req_err:
        print(f"An unexpected error occurred for {full_url}: {req_err}")
    return None

def process_observation(obs, weather_id, target_timezone):
    """
    Extracts data from a single weather observation,
    convert valid_time_gmt to correct timezone
    """
    processed_obs = {
        'weather_id': weather_id,
        'temp': obs.get('temp'),
        'wx_phrase': obs.get('wx_phrase'),
        'wdir_cardinal': obs.get('wdir_cardinal'),
        'wspd': obs.get('wspd'),
        'pressure': obs.get('pressure'),
        'precip_hrly': obs.get('precip_hrly'),
        'uv_index': obs.get('uv_index'),
        #can add additional fields from observations here at a later time if needed
    }

    valid_time_gmt_unix = obs.get("valid_time_gmt")
    if valid_time_gmt_unix is not None:
        try:
            # Convert Unix timestamp to timezone-aware UTC datetime
            valid_datetime_utc = datetime.fromtimestamp(valid_time_gmt_unix, tz=timezone.utc)
            # Convert UTC datetime to local timezone adjusted for daylight savings
            processed_obs['local_datetime'] = valid_datetime_utc.astimezone(target_timezone).replace(tzinfo=None)
        except Exception as e:
            print(f"Error converting timestamp {valid_time_gmt_unix} to datetime for weather_id {weather_id}: {e}")
            processed_obs['local_datetime'] = None # None on error
    else:
        processed_obs['local_datetime'] = None # No timestamp

    return processed_obs



# --- Main Script ---
if __name__ == "__main__":
    # Fixed start date and today's date
    START_DATE_FIXED_STR = '2023-08-20'
    TODAY_DATE_STR = datetime.now().strftime('%Y-%m-%d')
    MAX_DAYS_PER_CHUNK = 30
    WEATHER_URLS_FILE = 'weather_urls.csv'

    print(f"Fetching weather data from {START_DATE_FIXED_STR} to {TODAY_DATE_STR}")

    # Read the weather URLs and metadata from CSV
    url_data = pd.read_csv(WEATHER_URLS_FILE)


    all_weather_records = []

    # Generate date chunks for the period
    date_chunks = generate_date_chunks(START_DATE_FIXED_STR, TODAY_DATE_STR, MAX_DAYS_PER_CHUNK)

    # Iterate through each URL/location
    for index, row in url_data.iterrows():
        base_url = row['base_url']
        weather_id = row['weather_id']
        location_name = row['location_name']
        timezone_abbr = row['timezone']
        
        target_timezone = get_timezone_object(timezone_abbr)

        print(f"\n--- Processing Location: {location_name} (ID: {weather_id}, Timezone: {timezone_abbr}) ---")

        # Iterate through each date chunk for the location
        for chunk in date_chunks:
            start_date_param = chunk['start_date']
            end_date_param = chunk['end_date']

            print(f"  Fetching data for range: {start_date_param} to {end_date_param}")
            weather_json = fetch_weather_data_for_range(base_url, start_date_param, end_date_param)

            if weather_json and weather_json.get("observations"):
                for obs in weather_json["observations"]:
                    processed_record = process_observation(obs, weather_id, target_timezone)
                    all_weather_records.append(processed_record)
            else:
                print(f"  No observations found or error for {location_name} in range {start_date_param}-{end_date_param}")

            time.sleep(1) # Pause to avoid API limit

    # Convert records to dataframe
    if all_weather_records:
        final_weather_df = pd.DataFrame(all_weather_records)
        print(f"\nTotal records collected: {len(final_weather_df)}")

        #Save CSV file
        final_weather_df.to_csv('data/weather_data.csv', index=False)
        print("\nWeather data saved to data/weather_data.csv")

    else:
        print("\nNo weather data was collected.")