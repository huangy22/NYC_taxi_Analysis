#!/home/kun/anaconda2/bin python
import sys
import numpy as np
import pandas as pd
import datetime as dt
import fiona
from shapely import geometry
from sqlalchemy import create_engine # database connection

def rename_columns(df_trip, df_fare):
    ''' Rename the columns in data frame: remove the space in front of each name'''
    
    df_trip.columns = ['medallion', 'hack_license', 'vendor_id', 'rate_code', 'store_and_fwd_flag', 
                   'pickup_datetime', 'dropoff_datetime', 'passenger_count', 'trip_time', 'trip_distance', 
                   'pickup_long', 'pickup_lat', 'dropoff_long', 'dropoff_lat']
    df_fare.columns = ['medallion', 'hack_license', 'vendor_id', 'pickup_datetime', 
                   'payment_type', 'fare_amount', 'surcharge', 'mta_tax',
                   'tip_amount', 'tolls_amount', 'total_amount']
    return

def merge(df_trip, df_fare):
    ''' Merge two data frame together and remove duplicate columns '''
    df = pd.concat([df_trip, df_fare], axis=1, join='inner')
    df = df.loc[:,~df.columns.duplicated()]
    return df

def drop_wrong_data(df):
    ''' Remove rows with unreasonable values in passenger_count, trip_time, trip_distance, 
        total_amount, fare_amount, tip_amount, toll_amount, etc. '''
    
    # number of passenger: 208 is way too large to be true, 0 is also not for a valid ride
    df = df[df.passenger_count != 208]
    df = df[df.passenger_count != 0]

    # trip time should be non-negative and less than one day
    df = df[df.trip_time >= 0.0]
    df = df[df.trip_time <= 86400]
    
    # trip distance larger than 100 miles is unreasonably far 
    df = df[df.trip_distance < 100]
    
    # any amount paid should be non-negative
    df = df[df.total_amount >= 0.0]
    df = df[df.fare_amount >= 0.0]
    df = df[df.surcharge >= 0.0]
    df = df[df.tolls_amount >= 0.0]
    return

def find_zone(row, cols):
    lon = row[cols[0]]
    lat = row[cols[1]]
    point = geometry.Point(lon, lat)# longitude, latitude
    for i in idx_shape:
        bound = bounds[i]
        if lon < bound[0] or lon > bound[2] or lat < bound[1] or lat > bound[3]:
            continue

        # Alternative: if point.within(shape)
        if shapes[i].contains(point):
            return zones[i]
    return

def add_pickup_dropoff_zones(df):
    dfs = [df]

    pickup_cols = ['pickup_long', 'pickup_lat']
    dfs.append(df[pickup_cols].apply(
            lambda cell: pd.Series(find_zone(cell, pickup_cols), index=['pickup_LocationID']), axis=1))

    dropoff_cols = ['dropoff_long', 'dropoff_lat']
    dfs.append(df[dropoff_cols].apply(
            lambda cell: pd.Series(find_zone(cell, dropoff_cols), index=['dropoff_LocationID']), axis=1))

    return pd.concat(dfs, axis=1)


# df_trip = pd.read_csv("../via_data_challenge/original_data/trip_data/trip_data_8.csv")
# df_fare = pd.read_csv("../via_data_challenge/original_data/trip_fare/trip_fare_8.csv")

# rename_columns(df_trip, df_fare)
# df = merge(df_trip, df_fare)
# drop_wrong_data(df)

# df.to_csv("trip_data_2013_08.csv")

shapes = []
bounds = []
zones = []
with fiona.open("./taxi_zones/taxi_zones_new.shp") as fiona_collection:
    for zone in fiona_collection:
        # Use Shapely to create the polygon
        shape = geometry.asShape(zone['geometry'])
        shapes.append(shape)
        bounds.append(shape.bounds)
        zones.append(zone['properties']['LocationID'])

idx_shape = range(len(shapes))

start = dt.datetime.now()
chunksize = 20000
j = 0

job_id = sys.argv[1]
disk_engine = create_engine('sqlite:///trip_data_2013.db')

for df in pd.read_csv('./cleaned_data/trip_data_2013_'+str(job_id)+'.csv', header=0, chunksize=chunksize, iterator=True, encoding='utf-8'):
    df = add_pickup_dropoff_zones(df)

    df.to_sql("trip_data_"+str(job_id), disk_engine, if_exists='append')
    
    j+=1
    print '{} seconds: completed {} rows'.format((dt.datetime.now() - start).seconds, j*chunksize)


# with open("./inputs/"+job_id+"_input", 'r') as f:
    # inputs = f.readline().split(' ')
    # index_start = int(inputs[0])
    # index_end = int(inputs[1])

# index = index_start+1
# for df in pd.read_csv('./trip_data_2013_08.csv', header=0, chunksize=chunksize, skiprows=range(1, index_start+1), iterator=True, encoding='utf-8'):
    # df = add_pickup_dropoff_zones(df)

    # if j == 0:
        # with open(job_id+'_zone_trip_2013_08.csv', 'w') as f:
            # df.to_csv(f)
    # else:
        # with open(job_id+'_zone_trip_2013_08.csv', 'a') as f:
            # df.to_csv(f, header=False)
    
    # j+=1
    # print '{} seconds: completed {} rows'.format((dt.datetime.now() - start).seconds, index_start + j*chunksize)
    
    # index += chunksize
    # if index > index_end:
        # break

# for i in range(1, 12):
    # df = pd.read_csv(str(i)+'_zone_trip_2013_08.csv', header=0, low_memory=False)
    # print len(df)
    # print df.describe()
    # df.to_sql("trip_data", disk_engine, if_exists='append')

