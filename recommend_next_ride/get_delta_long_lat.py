#!/home/kun/anaconda2/bin python
import numpy as np
import pandas as pd
import seaborn as sns
from scipy import stats
import pickle
from math import sin, cos, sqrt, atan2, radians

from sqlalchemy import create_engine
from sqlalchemy.sql import text
import datetime as dt
from IPython.display import display

import matplotlib.pyplot as plt

def region(location_id):
    if location_id in Astoria_zones:
        return "AST"
    elif location_id in Laguardia_zones:
        return "LAG"
    elif location_id in Upper_east_zones:
        return "UES"
    else:
        return

def borough(location_id):
    if location_id in Manhattan_zones:
        return "MAN"
    else:
        return

def get_distance(lon1, lat1, lon2, lat2):
    # approximate radius of earth in km
    R = 6373.0

    r_lat1 = radians(lat1)
    r_lon1 = radians(lon1)
    r_lat2 = radians(lat2)
    r_lon2 = radians(lon2)

    dlon = r_lon2 - r_lon1
    dlat = r_lat2 - r_lat1

    a = sin(dlat / 2)**2 + cos(r_lat1) * cos(r_lat2) * sin(dlon / 2)**2
    c = 2 * atan2(sqrt(a), sqrt(1 - a))

    return R * c

def query_rides_region(zones):
    # query all the rides coming out of the regions
    query_rides = '''SELECT pickup_datetime, dropoff_datetime, pickup_LocationID, pickup_long, pickup_lat,
                            dropoff_LocationID, dropoff_long, dropoff_lat
                     FROM trip_data
                     WHERE pickup_LocationID IN ({zone_ids})
                        OR dropoff_LocationID IN ({zone_ids})
                     ORDER BY pickup_datetime ASC, dropoff_datetime ASC'''
    f = {"zone_ids": ",".join([str(i) for i in zones])}
    return pd.read_sql_query(query_rides.format(**f), disk_engine)
    
if __name__ == "__main__":
    DEBUG = True

    disk_engine = create_engine('sqlite:///trip_data_2013.db')

    zones = pd.read_csv("./taxi_zones/taxi_zone_lookup.csv")
    
    Manhattan_zones = zones[zones.Borough=="Manhattan"]["LocationID"].tolist()
    Astoria_zones = zones[[row in ["Astoria", "Old Astoria", "Astoria Park"] 
                       for row in zones.Zone]]["LocationID"].tolist()
    Laguardia_zones = zones[zones.Zone == "LaGuardia Airport"]["LocationID"].tolist()
    Upper_east_zones = zones[[row in ["Upper East Side North", "Upper East Side South"] 
                          for row in zones.Zone]]["LocationID"].tolist()

    df_astoria = query_rides_region(Astoria_zones)
    print df_astoria.dropoff_lat.max(), df_astoria.dropoff_lat.min()
    print df_astoria.dropoff_long.max(), df_astoria.dropoff_long.min()

    lat = 40.0
    lon = -74.0
    delta_lat = 0.001

    for i in range(1, 100):
        del_lat = delta_lat*i
        print "del_lon: ", del_lat, get_distance(lon, lat, lon, lat+del_lat)

