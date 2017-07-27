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

    for df in pd.read_csv('./cleaned_data/trip_data_2013_'+str(job_id)+'.csv', header=0, chunksize=chunksize, iterator=True, encoding='utf-8'):
        df = add_pickup_dropoff_zones(df)

        df.to_sql("trip_data_"+str(job_id), disk_engine, if_exists='append')
        
        j+=1
        print '{} seconds: completed {} rows'.format((dt.datetime.now() - start).seconds, j*chunksize)
