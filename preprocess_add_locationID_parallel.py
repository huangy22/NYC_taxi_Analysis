#!/home/kun/anaconda2/bin python
import sys
import numpy as np
import pandas as pd
import datetime as dt
import fiona
from shapely import geometry
from sqlalchemy import create_engine # database connection

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

if __name__ == "__main__":

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

    with open("./inputs/"+job_id+"_input", 'r') as f:
        inputs = f.readline().split(' ')
        index_start = int(inputs[0])
        index_end = int(inputs[1])

    index = index_start+1
    for df in pd.read_csv('./trip_data_2013_08.csv', header=0, chunksize=chunksize, skiprows=range(1, index_start+1), iterator=True, encoding='utf-8'):
        df = add_pickup_dropoff_zones(df)

        if j == 0:
            with open(job_id+'_zone_trip_2013_08.csv', 'w') as f:
                df.to_csv(f)
        else:
            with open(job_id+'_zone_trip_2013_08.csv', 'a') as f:
                df.to_csv(f, header=False)
        
        j+=1
        print '{} seconds: completed {} rows'.format((dt.datetime.now() - start).seconds, index_start + j*chunksize)
        
        index += chunksize
        if index > index_end:
            break

    for i in range(1, 12):
        df = pd.read_csv(str(i)+'_zone_trip_2013_08.csv', header=0, low_memory=False)
        print len(df)
        print df.describe()
        df.to_sql("trip_data", disk_engine, if_exists='append')

