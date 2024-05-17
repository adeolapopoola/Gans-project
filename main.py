import functions_framework
import requests
import my_stuff
import pandas as pd
from datetime import datetime,timedelta
from pytz import timezone

@functions_framework.http
def insert(request):
    con_string = connection()
    city_df = get_cities(con_string)
    airport_df = get_airport(con_string)
    get_weather_information(city_df,con_string)
    get_flight_info (airport_df, con_string)
    return 'Data successfully added'

def connection():
      connection_name = "commanding-time-422616-a0:europe-west1:wbs-mysql-db"
      db_user = "root"
      db_password = my_stuff.my_sql_password
      schema_name = "Gans"

      driver_name = 'mysql+pymysql'
      query_string = dict({"unix_socket": f"/cloudsql/{connection_name}"})

      con_string = sqlalchemy.create_engine(
          sqlalchemy.engine.url.URL(
              drivername = driver_name,
              username = db_user,
              password = db_password,
              database = schema_name,
              query = query_string,
          )
      )
      return con_string


def connection():
    schema = "Gans"
    host = "34.38.20.218"
    user = "root"
    password =my_stuff.my_sql_password
    port = 3306
    connection_string = f'mysql+pymysql://{user}:{password}@{host}:{port}/{schema}'
    return connection_string

def get_cities(con_string):
    city_df = pd.read_sql('city', con= con_string)
    return city_df

def get_airport(con_string):
    airport_df = pd.read_sql('airport', con= con_string)
    return airport_df

def get_weather_information(city_df, con_string):
    berlin_timezone = timezone('Europe/Berlin')
    weather_info= {"city_id":[],
                "city":[],
                "Time_stamp":[],
                "Temperature":[],
                "Humidity":[],
                "Outlook": [],
                "Rain_probability":[],
                "Wind_speed":[],
                "Date_retrieved":[]
                }
    for i,row in city_df.iterrows():
        url= (f"http://api.openweathermap.org/data/2.5/forecast?q={row['city']}&appid={my_stuff.my_API_Key}&units=metric")
        response= requests.get(url)
        weather_details = response.json()
        timestamp= datetime.now(berlin_timezone).strftime("%Y-%m-%d %H:%M:%S")
    
        for info in weather_details["list"][:8]:
            weather_info["city_id"].append(row["city_id"])
            weather_info["city"].append(row["city"])
            weather_info["Time_stamp"].append(info.get("dt_txt", None)),
            weather_info["Temperature"].append(info["main"].get("temp", None)),
            weather_info["Humidity"].append(info["main"].get("humidity", None)),
            weather_info["Outlook"].append(info["weather"][0].get("main", None)),
            weather_info["Rain_probability"].append(info.get("rain", {}).get("3h", 0)),
            weather_info["Wind_speed"].append(info["wind"].get("speed", None)),
            weather_info["Date_retrieved"].append(timestamp)

    weather_df =pd.DataFrame(weather_info)
    weather_df.to_sql('weather',if_exists = 'append', con =con_string, index = False)

def get_flight_info (airport_df, con_string):
    
    now = datetime.now()
    tomorrow = now + timedelta(days=1)
    tomorrow = tomorrow.strftime('%Y-%m-%d')
    times_list = [{'from':'00:00','to':'11:59'},{'from':'12:00','to':'23:59'}]
    flight_info = {
                 'flight_number':[],
                 'departure_airpot_name': [],
                 'arrival_ICAO':[],
                 'arrival_terminal':[],
                 'arrival_gate':[],
                 'arrival_time':[],
                 'flight_status':[]
                       }  
    for i,row in airport_df.iterrows():
        for time in times_list:
            url = f"https://aerodatabox.p.rapidapi.com/flights/airports/icao/{row['icao']}/{tomorrow}T{time['from']}/{tomorrow}T{time['to']}"
            querystring = {"withLeg":"true","withCancelled":"true","withCodeshared":"true","withCargo":"true","withPrivate":"true","withLocation":"false"}
            headers = {
            	"X-RapidAPI-Key": my_stuff.my_Rapid_Key,
            	"X-RapidAPI-Host": "aerodatabox.p.rapidapi.com"
            }
            response = requests.get(url, headers=headers, params=querystring)
            flight_response= response.json()
            
            for info in flight_response['arrivals']:
                
                flight_info['flight_number'].append(info.get('number', None)),
                flight_info['departure_airpot_name'].append(info['departure']['airport'].get('name',None)),
                flight_info['arrival_ICAO'].append(row['icao']),
                flight_info['arrival_terminal'].append(info['arrival'].get('terminal', None)),
                flight_info['arrival_gate'].append(info['arrival'].get('gate', None)),
                flight_info['arrival_time'].append(info['arrival']['scheduledTime'].get('local', None)),
                flight_info['flight_status'].append(info.get('status',None))
                    
    flight_info = pd.DataFrame(flight_info)
    flight_info['arrival_time']= flight_info['arrival_time'].str[:16]
    flight_info.to_sql('flights',con=con_string, if_exists='append', index=False )
    