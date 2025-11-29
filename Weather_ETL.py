import requests
import pandas as pd
import sqlite3
from datetime import datetime
import os
import time

class WeatherETL:
    def __init__(self, api_key):
        self.api_key = api_key
        self.base_url = "http://api.openweathermap.org/data/2.5/weather"
        # Maharashtra Districts
        self.maharashtra_districts = [
            'Ahmednagar', 'Akola', 'Amravati', 'Aurangabad', 'Beed', 'Bhandara', 
            'Buldhana', 'Chandrapur', 'Dhule',  'Hingoli', 
            'Jalgaon', 'Jalna', 'Kolhapur', 'Latur', 'Mumbai', 'Thane', 
            'Nagpur', 'Nandurbar', 'Nashik', 'Osmanabad', 'Palghar', 
            'Parbhani', 'Pune', 'Raigad', 'Ratnagiri', 'Sangli', 'Satara', 
            'Solapur', 'Thane', 'Wardha', 'Washim', 'Yavatmal'
        ]
        # Nashik Sub-districts/Talukas
        self.nashik_subdistricts = [
            'Trimbakeshwar', 'Igatpuri', 'Dindori', 'Peth',
            'Surgana', 'Kalwan', 'Deola', 'Baglan', 'Malegaon', 'Nandgaon',
            'Chandwad', 'Yeola', 'Niphad', 'Sinnar'
        ]

    def extract_weather_data(self, locations):
        weather_data = []
        failed_locations = []
        max_retries = 3
        for location in locations:
            for attempt in range(max_retries):
                time.sleep(1)  # Simple rate limit
                # Build a search-friendly location string
                if location == "Trimbakeshwar":
                    search_location = "Trimbak,Maharashtra,IN"
                elif location == "Sindhudurg":
                    search_location = "Sindhudurg Nagari,Maharashtra,IN"
                elif location == "Raigad":
                    search_location = "Alibag,Maharashtra,IN"
                elif location == "Gadchiroli":
                    search_location = "Gadchiroli,Maharashtra,IN"
                elif location == "Chandwad":
                    search_location = "Chandvad,Maharashtra,IN"
                elif location in self.nashik_subdistricts:
                    search_location = f"{location},Nashik,Maharashtra,IN"
                else:
                    search_location = f"{location},Maharashtra,IN"
                try:
                    params = {'q': search_location, 'appid': self.api_key, 'units': 'metric'}
                    response = requests.get(self.base_url, params=params, timeout=10)
                    response.raise_for_status()
                    data = response.json()
                    weather_info = {
                        'location_name': location,
                        'district': 'Nashik' if location in self.nashik_subdistricts else location,
                        'sub_district': location if location in self.nashik_subdistricts else 'N/A',
                        'temperature': data.get('main', {}).get('temp'),
                        'feels_like': data.get('main', {}).get('feels_like'),
                        'min_temp': data.get('main', {}).get('temp_min'),
                        'max_temp': data.get('main', {}).get('temp_max'),
                        'humidity': data.get('main', {}).get('humidity'),
                        'pressure': data.get('main', {}).get('pressure'),
                        'weather_desc': data.get('weather', [{}])[0].get('description'),
                        'wind_speed': data.get('wind', {}).get('speed'),
                        'wind_direction': data.get('wind', {}).get('deg', 0),
                        'cloudiness': data.get('clouds', {}).get('all'),
                        'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                        'sunrise': datetime.fromtimestamp(data.get('sys', {}).get('sunrise', 0)).strftime('%H:%M:%S') if data.get('sys') else None,
                        'sunset': datetime.fromtimestamp(data.get('sys', {}).get('sunset', 0)).strftime('%H:%M:%S') if data.get('sys') else None,
                        'latitude': data.get('coord', {}).get('lat'),
                        'longitude': data.get('coord', {}).get('lon'),
                        'error': None
                    }
                    weather_data.append(weather_info)
                    print(f"Successfully extracted data for {location}")
                    break
                except requests.exceptions.RequestException as e:
                    print(f"Error fetching data for {location}: {str(e)}")
                    error_msg = str(e)
                    if attempt == max_retries - 1:
                        failed_locations.append(location)
                        placeholder = {
                            'location_name': location,
                            'district': 'Nashik' if location in self.nashik_subdistricts else location,
                            'sub_district': location if location in self.nashik_subdistricts else 'N/A',
                            'temperature': None, 'feels_like': None, 'min_temp': None, 'max_temp': None,
                            'humidity': None, 'pressure': None, 'weather_desc': None,
                            'wind_speed': None, 'wind_direction': None, 'cloudiness': None,
                            'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                            'sunrise': None, 'sunset': None,
                            'latitude': None, 'longitude': None, 'error': error_msg
                        }
                        weather_data.append(placeholder)
                    else:
                        time.sleep(2)
                        continue
                except Exception as e:
                    print(f"Unexpected error for {location}: {str(e)}")
                    if attempt == max_retries - 1:
                        failed_locations.append(location)
                        placeholder = {
                            'location_name': location,
                            'district': 'Nashik' if location in self.nashik_subdistricts else location,
                            'sub_district': location if location in self.nashik_subdistricts else 'N/A',
                            'temperature': None, 'feels_like': None, 'min_temp': None, 'max_temp': None,
                            'humidity': None, 'pressure': None, 'weather_desc': None,
                            'wind_speed': None, 'wind_direction': None, 'cloudiness': None,
                            'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                            'sunrise': None, 'sunset': None,
                            'latitude': None, 'longitude': None, 'error': str(e)
                        }
                        weather_data.append(placeholder)
                    else:
                        time.sleep(2)
            # Optional: print failed summary at the end of all locations
        if failed_locations:
            print("\nFailed to fetch data for the following locations:")
            for location in failed_locations:
                print(f"- {location}")
            print(f"\nTotal failed locations: {len(failed_locations)}")
        return weather_data

    def transform_data(self, weather_data):
        if not weather_data:
            return None
        df = pd.DataFrame(weather_data)
        numeric_columns = ['temperature', 'feels_like', 'min_temp', 'max_temp', 'wind_speed', 'latitude', 'longitude']
        for col in numeric_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce').round(2)
        if 'weather_desc' in df.columns:
            df['weather_desc'] = df['weather_desc'].str.title()
        if 'temperature' in df.columns:
            df['temperature_fahrenheit'] = (df['temperature'] * 9/5 + 32).round(2)
        def get_comfort_level(row):
            try:
                temp = row['temperature']
                humidity = row['humidity']
                if pd.isna(temp) or pd.isna(humidity):
                    return 'Unknown'
                if temp < 20:
                    return 'Cool'
                elif temp > 35:
                    return 'Very Hot'
                elif humidity > 80:
                    return 'Humid'
                elif temp >= 20 and temp <= 28 and humidity >= 40 and humidity <= 60:
                    return 'Comfortable'
                else:
                    return 'Moderate'
            except:
                return 'Unknown'
        df['comfort_level'] = df.apply(get_comfort_level, axis=1)
        def get_wind_direction(degrees):
            try:
                if pd.isna(degrees):
                    return 'Unknown'
                directions = ['N', 'NNE', 'NE', 'ENE', 'E', 'ESE', 'SE', 'SSE',
                            'S', 'SSW', 'SW', 'WSW', 'W', 'WNW', 'NW', 'NNW']
                index = round(float(degrees) / (360 / len(directions))) % len(directions)
                return directions[index]
            except:
                return 'Unknown'
        if 'wind_direction' in df.columns:
            df['wind_direction_desc'] = df['wind_direction'].apply(get_wind_direction)
        return df

    def load_to_csv(self, df, output_file):
        try:
            df.to_csv(output_file, index=False)
            print(f"\nData successfully saved to {output_file}")
            print("\nData Validation Summary:")
            print(f"Total locations covered: {len(df)}")
            print(f"Districts covered: {len(df[df['sub_district'] == 'N/A'])}")
            print(f"Nashik sub-districts covered: {len(df[df['district'] == 'Nashik'])}")
            print(f"Temperature range: {df['temperature'].min():.1f}°C to {df['temperature'].max():.1f}°C")
            print(f"Average temperature: {df['temperature'].mean():.1f}°C")
            all_locations = self.maharashtra_districts + self.nashik_subdistricts
            collected_locations = set(df['location_name'])
            missing_locations = set(all_locations) - collected_locations
            if missing_locations:
                print("\nMissing locations:")
                for loc in sorted(missing_locations):
                    print(f"- {loc}")
        except Exception as e:
            print(f"Error saving to CSV: {str(e)}")

    def load_to_sqlite(self, df, db_name, table_name):
        try:
            conn = sqlite3.connect(db_name)
            df.to_sql(table_name, conn, if_exists='append', index=False)
            conn.close()
            print(f"Data successfully loaded to SQLite database: {db_name}, table: {table_name}")
        except Exception as e:
            print(f"Error saving to SQLite: {str(e)}")

def main():
    # OpenWeatherMap API key (free tier)
    API_KEY = "ADD_OPENWEATHERMAP_API_KEY_HERE"
    # Initialize ETL pipeline
    weather_etl = WeatherETL(API_KEY)
    locations = list(set(weather_etl.maharashtra_districts + weather_etl.nashik_subdistricts))
    print("Extracting weather data...")
    weather_data = weather_etl.extract_weather_data(locations)
    print("\nTransforming data...")
    transformed_data = weather_etl.transform_data(weather_data)
    if transformed_data is not None:
        output_dir = 'output'
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M")
        csv_file = os.path.join(output_dir, f'maharashtra_weather_{timestamp}.csv')
        weather_etl.load_to_csv(transformed_data, csv_file)
        db_file = os.path.join(output_dir, 'maharashtra_weather.db')
        weather_etl.load_to_sqlite(transformed_data, db_file, 'weather_data')
        print("\nETL pipeline completed successfully!")
    else:
        print("No data to process. ETL pipeline stopped.")

if __name__ == "__main__":
    main()

