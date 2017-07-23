#!/home/kun/anaconda2/bin python
import sys
import numpy as np
import pandas as pd
import datetime as dt
import fiona
from shapely import geometry
from shapely.ops import cascaded_union
from sqlalchemy import create_engine # database connection

#used to plot the zones 
from matplotlib import pyplot as plt
from descartes import PolygonPatch

def find_zone(row, cols):
    lon = row[cols[0]]
    lat = row[cols[1]]
    point = geometry.Point(lon, lat)# longitude, latitude
    for zone in ZONES:
        bound = zone[1]
        if lon < bound[0] or lon > bound[2] or lat < bound[1] or lat > bound[3]:
            continue

        # Alternative: if point.within(shape)
        if zone[0].contains(point):
            return zone[2]
    return

# def find_borough(row, cols):
    # lon = row[cols[0]]
    # lat = row[cols[1]]
    # point = geometry.Point(lon, lat)# longitude, latitude
    # for borough in BORUOGHS:
        # bound = borough[1]
        # if lon < bound[0] or lon > bound[2] or lat < bound[1] or lat > bound[3]:
            # continue

        # # Alternative: if point.within(shape)
        # if borough[0].contains(point):
            # return borough[2]
    # return

# def find_region(row, cols):
    # lon = row[cols[0]]
    # lat = row[cols[1]]
    # point = geometry.Point(lon, lat)# longitude, latitude
    # for region in REGIONS:
        # bound = region[1]
        # if lon < bound[0] or lon > bound[2] or lat < bound[1] or lat > bound[3]:
            # continue

        # # Alternative: if point.within(shape)
        # if region[0].contains(point):
            # return region[2]
    # return

def get_region(zone):
    if zone in astoria:
        return "AST"
    if zone in laguardia:
        return "LAG"
    if zone in upper_east_side:
        return "AST"
    return

def get_borough(zone):
    if zone in manhattan:
        return "MAN"
    return

def add_pickup_dropoff_zones(df):

    # apply find_borough function to each row to find out the pickup borough
    pickup_cols = ['pickup_long', 'pickup_lat']
    dropoff_cols = ['dropoff_long', 'dropoff_lat']

    new_cols = df.columns.tolist() + ['pickup_borough', 'pickup_region', 'dropoff_borough', 'dropoff_region']
    df_region = pd.DataFrame(columns=new_cols)

    for index, row in df.iterrows():
        # pickup_borough = find_borough(row, pickup_cols)
        # pickup_region = find_region(row, pickup_cols)

        # dropoff_borough = find_borough(row, dropoff_cols)
        # dropoff_region = find_region(row, dropoff_cols)

        pickup_zone = find_zone(row, pickup_cols)
        dropoff_zone = find_zone(row, dropoff_cols)

        pickup_region = get_region(pickup_zone)
        dropoff_region = get_region(dropoff_zone)
        
        pickup_borough = get_borough(pickup_zone)
        dropoff_borough = get_borough(dropoff_zone)

        if pickup_region or dropoff_region:
            value = row.tolist()+[pickup_borough, pickup_region, dropoff_borough, dropoff_region]
            df_region.loc[df_region.shape[0]] = value
    return df_region

def plot_region(polygon):
    """ plot a polygon for debug purpose
    """

    BLUE = '#6699cc'
    fig, ax = plt.subplots()
    # polygon = geometry.Polygon([ (10, 3), (0, 0), (5, 0)])
    patch = PolygonPatch(polygon, fc=BLUE, ec=BLUE, alpha=1, zorder=2)
    ax.add_patch(patch)
    bound=polygon.bounds
    ax.set_xlim([bound[0],bound[2]])
    ax.set_ylim([bound[1],bound[3]])
    plt.show()

def merge_region(zone_rows):
    """ given a list of zones and return a merged shape
    """
    id_list = zone_rows["LocationID"].tolist()
    zone_list=[shape for shape, bounds, zone in ZONES if zone in id_list]
    #join all zones in manhattan
    region=cascaded_union(zone_list)
    return region

if __name__=="__main__":

    #load zone look up table
    zone_lookup=pd.read_csv("./taxi_zones/taxi_zone_lookup.csv")

    manhattan = zone_lookup[zone_lookup["Borough"]=="Manhattan"]["LocationID"].tolist()
    astoria = zone_lookup[[row in ["Astoria", "Astoria Park", "Old Astoria"] for row in zone_lookup["Zone"]]]["LocationID"].tolist()
    upper_east_side = zone_lookup[[row in ["Upper East Side North", "Upper East Side South"] for row in zone_lookup["Zone"]]]["LocationID"].tolist()
    laguardia = zone_lookup[zone_lookup["Zone"] == "LaGuardia Airport"]["LocationID"].tolist()

    all_zones = manhattan + astoria + upper_east_side + laguardia

    ZONES = []
    with fiona.open("./taxi_zones/taxi_zones_new.shp") as fiona_collection:
        for zone in fiona_collection:
            # Use Shapely to create the polygon
            shape = geometry.asShape(zone['geometry'])
            if zone["properties"]["LocationID"] in all_zones:
                ZONES.append((shape, shape.bounds, zone["properties"]["LocationID"]))
    
    # BORUOGHS = []
    # REGIONS = []

    # #find the Manhattan borough
    # manhattan=zone_lookup.loc[zone_lookup["Borough"]=="Manhattan"]
    # manhattan_region = merge_region(manhattan)
    # BORUOGHS.append((manhattan_region, manhattan_region.bounds, 'MAN'))
    # # plot_region(manhattan_region)

    # #find the Astoria region
    # astoria = zone_lookup.loc[[row in ["Astoria", "Astoria Park", "Old Astoria"] for row in zone_lookup["Zone"]]]
    # astoria_region = merge_region(astoria)
    # REGIONS.append((astoria_region, astoria_region.bounds, 'AST'))
    # # plot_region(astoria_region)

    # #find the Upper East Side region
    # upper_east_side = zone_lookup.loc[[row in ["Upper East Side North", "Upper East Side South"] for row in zone_lookup["Zone"]]]
    # upper_east_region = merge_region(upper_east_side)
    # REGIONS.append((upper_east_region, upper_east_region.bounds, 'UES'))
    # # plot_region(upper_east_region)

    # #find the LaGuardia Airport region
    # laguardia = zone_lookup.loc[zone_lookup["Zone"] == "LaGuardia Airport"]
    # laguardia_region = merge_region(laguardia)
    # REGIONS.append((laguardia_region, laguardia_region.bounds, 'LAG'))
    # # plot_region(laguardia_region)

    # sys.exit(0)

    start = dt.datetime.now()
    chunksize = 20000

    # with open("./inputs/"+job_id+"_input", 'r') as f:
        # inputs = f.readline().split(' ')
        # index_start = int(inputs[0])
        # index_end = int(inputs[1])

    job_id = sys.argv[1]
    j = 0
    for df in pd.read_csv('./cleaned_data/trip_data_2013_{0}.csv'.format(job_id), header=0, chunksize=chunksize, iterator=True, encoding='utf-8'):

        df = add_pickup_dropoff_zones(df)
        if j == 0:
            df_zone = df
        else:
            df_zone = pd.concat([df_zone, df], axis=0, ignore_index=True)
        
        j+=1
        print '{} seconds: completed {} rows'.format((dt.datetime.now() - start).seconds, j*chunksize)
        print df_zone.shape[0]
        
        # if index > index_end:
            # break

    df_zone.to_csv('./zone_trip_data_2013_{0}.csv'.format(job_id))

