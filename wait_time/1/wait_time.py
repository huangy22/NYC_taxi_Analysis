import numpy as np
import pandas as pd
import seaborn as sns
from scipy import stats
import pickle
import sys
import itertools

from sqlalchemy import create_engine # database connection
from sqlalchemy.sql import text
import datetime as dt

ncpu=int(sys.argv[1])
pid=int(sys.argv[2])
month=int(sys.argv[3])
print "Calculate PID {0} with {1} cpus".format(pid, ncpu) 

def query_medallions():
    query = '''SELECT DISTINCT medallion
                    FROM trip_data_{month}'''
    f = {"month":month}
    return pd.read_sql_query(query.format(**f), disk_engine)


disk_engine = create_engine('sqlite:////work/via/trip_data_2013.db')
print "SQL initialized"

zones = pd.read_csv("/work/via/taxi_zones/taxi_zone_lookup.csv")
print zones.columns

Manhattan_zones = zones[zones.Borough=="Manhattan"]["LocationID"].tolist()
Astoria_zones = zones[[row in ["Astoria", "Old Astoria", "Astoria Park"] for row in zones.Zone]]["LocationID"].tolist()
Laguardia_zones = zones[zones.Zone == "LaGuardia Airport"]["LocationID"].tolist()
UpperEast_zones = zones[[row in ["Upper East Side North", "Upper East Side South"] for row in zones.Zone]]["LocationID"].tolist()

#medallions = pickle.load(open("/work/via/wait_time/medallions_all.txt", "r"))
medallions = query_medallions()
medallions = medallions.medallion.tolist()
print len(medallions)

# sys.exit(0)

# # medallions=medallions[:500]
interval=len(medallions)/(ncpu-1)
chuncks=medallions[pid*interval:(pid+1)*interval]
print len(chuncks)

# medallions=medallions[:500]
# pid=0
# ncpus=1
# chuncks=medallions
# print len(chuncks)

# query the rides information table for future use
zones=UpperEast_zones+Laguardia_zones+Astoria_zones
df_wait = pd.DataFrame(columns =["pickup_datetime", "dropoff_datetime", "pickup_LocationID", "pickup_long", "pickup_lat",
                                 "dropoff_LocationID", "dropoff_long", "dropoff_lat", "medallion", "wait_time", "delta_long", "delta_lat"])
count=0
for med in chuncks:
    count+=1
    if count%10==0:
        print "PID {0}: {1} of {2} is calculated".format(pid, count, len(chuncks))
    query_taxi = '''SELECT pickup_datetime, dropoff_datetime, pickup_LocationID, pickup_long, pickup_lat,
                                    dropoff_LocationID, dropoff_long, dropoff_lat, medallion
                    FROM trip_data_{month}
                    WHERE medallion = {medallion_id}
                    ORDER BY pickup_datetime ASC, dropoff_datetime ASC'''
    f = {"medallion_id": '"{0}"'.format(med), "month":month}
    df_taxi = pd.read_sql_query(query_taxi.format(**f), disk_engine)
    # print df_taxi.head()

    for index, row in df_taxi.iterrows():
        if row.dropoff_LocationID in zones:
            try:
                next_ride = df_taxi.iloc[index+1]
            except:
                next_ride = []
            if len(next_ride) > 0:
                wait_time = (pd.to_datetime(next_ride.pickup_datetime) - pd.to_datetime(row["dropoff_datetime"])).total_seconds()/60.0

                if wait_time > 0 and wait_time < 360.0:
                    dlong = next_ride.pickup_long - row.dropoff_long
                    dlat = next_ride.pickup_lat - row.dropoff_lat
                    row["wait_time"] = wait_time
                    row["delta_long"] = dlong
                    row["delta_lat"] = dlat
                    df_wait.loc[df_wait.shape[0]] = row   
    # print df_wait.head()

df_wait.to_csv("wait_time_distance_{0}.csv".format(pid))


