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


if __name__=="__main__":
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

    # with open("./inputs/"+job_id+"_input", 'r') as f:
        # inputs = f.readline().split(' ')
        # index_start = int(inputs[0])
        # index_end = int(inputs[1])

    job_id = sys.argv[1]
    j = 0
    for df in pd.read_csv('./trip_data_2013_{0}.csv'.format(job_id), header=0, chunksize=chunksize, iterator=True, encoding='utf-8'):

        if j == 0:
            df_zone = add_pickup_dropoff_zones(df)
        else:
            df_zone.append(add_pickup_dropoff_zones(df))
        
        j+=1
        print '{} seconds: completed {} rows'.format((dt.datetime.now() - start).seconds, j*chunksize)
        
        # if index > index_end:
            # break

    df_zone.to_csv('./zone_trip_data_2013_{0}.csv'.format(job_id))

