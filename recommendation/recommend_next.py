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

# [+/- Del_lon, +/- Del_lat] defines a square with 3km length and 3km width
# Results are calculated using "./get_delta_long_lat.py"
DEL_LON = 0.017
DEL_LAT = 0.013
R_LON_LAT = 0.017/0.013

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

def query_rides(zones, is_dropoff):
    if is_dropoff:
        # query all the rides coming in the regions
        query_rides = '''SELECT pickup_datetime, dropoff_datetime, pickup_LocationID, pickup_long, pickup_lat,
                                dropoff_LocationID, dropoff_long, dropoff_lat, passenger_count
                        FROM trip_data
                        WHERE dropoff_LocationID IN ({zone_ids})
                        ORDER BY pickup_datetime ASC, dropoff_datetime ASC'''
    else:
        # query all the rides coming out of the regions
        query_rides = '''SELECT pickup_datetime, dropoff_datetime, pickup_LocationID, pickup_long, pickup_lat,
                                dropoff_LocationID, dropoff_long, dropoff_lat, passenger_count
                        FROM trip_data
                        WHERE pickup_LocationID IN ({zone_ids})
                        ORDER BY pickup_datetime ASC, dropoff_datetime ASC'''

    f = {"zone_ids": ",".join([str(i) for i in zones])}
    return pd.read_sql_query(query_rides.format(**f), disk_engine)

def within_dist(box, pickup_long, pickup_lat):
    bool_lon = pickup_long > box[0] and pickup_long < box[1]
    bool_lat = pickup_lat > box[2] and pickup_lat < box[3]
    # print bool_lat and bool_lon
    return bool_lon and bool_lat

def find_best(lon, lat, df_candidates):
    # if len(df)<=0:
        # return None, None
    # candidate_dist=1.0e10
    # candidate_index=None
    # for index, row in df.iterrows():
        # prop_dist= abs(row["pickup_long"] - lon)+abs(row["pickup_lat"]-lat)
        # if prop_dist<candidate_dist:
            # candidate_index=index
    # best=df[candidate_index]
    # return best, get_distance(lon, lat, best.pickup_long, best.pickup_lat)

    df = df_candidates.copy()
    df["d_lon"]=abs(df["pickup_long"]-lon)
    df["d_lat"]=abs(df["pickup_lat"]-lon)
    df["lon_lat"] = df["d_lon"] + R_LON_LAT *df["d_lat"]
    df.sort_values("lon_lat", ascending=1)
    best = df.iloc[0]
    return best, get_distance(lon, lat, best.pickup_long, best.pickup_lat)
    
if __name__ == "__main__":
    DEBUG = True

    disk_engine = create_engine('sqlite:////work/via/trip_data_2013.db')

    zones = pd.read_csv("../taxi_zones/taxi_zone_lookup.csv")
    
    Manhattan_zones = zones[zones.Borough=="Manhattan"]["LocationID"].tolist()
    Astoria_zones = zones[[row in ["Astoria", "Old Astoria", "Astoria Park"] 
                       for row in zones.Zone]]["LocationID"].tolist()
    Laguardia_zones = zones[zones.Zone == "LaGuardia Airport"]["LocationID"].tolist()
    Upper_east_zones = zones[[row in ["Upper East Side North", "Upper East Side South"] 
                          for row in zones.Zone]]["LocationID"].tolist()

    # zones = [Upper_east_zones]
    # zone_name = ["UES"]

    # zones = [Astoria_zones]
    # zone_name = ["AST"]

    zones = [Laguardia_zones]
    zone_name = ["LAG"]
    for i in range(len(zones)):

        # df_dropoff = query_rides(zones[i], is_dropoff=True)
        # df_pickup = query_rides(zones[i], is_dropoff=False)

        # df_dropoff["dropoff_datetime"] = df_dropoff["dropoff_datetime"].apply(lambda x: pd.to_datetime(x))
        # print len(df_dropoff)

        # df_pickup["pickup_datetime"] = df_dropoff["pickup_datetime"].apply(lambda x: pd.to_datetime(x))
        # # df_pickup = df_pickup.set_index(pd.DatetimeIndex(df_pickup['pickup_datetime']))
        # print len(df_pickup)

        # df_pickup.to_csv("tmp_pickup.csv")
        # df_dropoff.to_csv("tmp_dropoff.csv")

        df_pickup = pd.read_csv("tmp_pickup.csv")
        df_dropoff = pd.read_csv("tmp_dropoff.csv")

        df_dropoff["dropoff_datetime"] = df_dropoff["dropoff_datetime"].apply(lambda x: pd.to_datetime(x))
        print len(df_dropoff)

        df_pickup["pickup_datetime"] = df_dropoff["pickup_datetime"].apply(lambda x: pd.to_datetime(x))
        df_pickup = df_pickup.set_index(pd.DatetimeIndex(df_pickup['pickup_datetime']))
        print len(df_pickup)
        
        delta_prev=dt.timedelta(minutes=5)
        delta_next=dt.timedelta(minutes=10)

        df_recommend = pd.DataFrame(columns = df_dropoff.columns.tolist() + 
                 ["region", "opt_wait_time", "next_pickup_lat", "next_pickup_long", "next_passenger_count", "next_pickup_dist"])

        for index, row in df_dropoff[:10].iterrows():

            start_time = row.dropoff_datetime - delta_prev 
            end_time = row.dropoff_datetime + delta_next

            box = [row.dropoff_long-DEL_LON, row.dropoff_long+DEL_LON, row.dropoff_lat-DEL_LAT, row.dropoff_lat+DEL_LAT]

            candidates = df_pickup[(df_pickup.pickup_datetime > start_time) & (df_pickup.pickup_datetime < end_time)]
            if len(candidates)>0:
                candidates = candidates[candidates.apply(lambda x: within_dist(box, x.pickup_long, x.pickup_lat), axis=1)]
            else:
                continue
            # candidates = candidates[candidates.apply(lambda x: 1>0, axis=1)]

            if DEBUG and index%10 == 0:
                print "{0} of {1}".format(index, df_dropoff.shape[0])
                # print candidates.head()

            if len(candidates) > 0:
                best, dist = find_best(row.dropoff_long, row.dropoff_lat, candidates)
                row["next_pickup_dist"] = dist
                row["next_pickup_long"] = best["pickup_long"]
                row["next_pickup_lat"] = best["pickup_lat"]
                row["opt_wait_time"] = (best["pickup_datetime"].to_pydatetime()-row["dropoff_datetime"]).total_seconds()/60.0
                row["next_passenger_count"] = best["passenger_count"]

                df_recommend.loc[df_recommend.shape[0]] = row

        print "Success rate : ", len(df_recommend)*1.0/len(df_dropoff)*100.0, "%"
        df_recommend["region"] = zone_name[i]

        df_recommend.to_csv(zone_name[i]+"_recommend_next_rides.csv")

