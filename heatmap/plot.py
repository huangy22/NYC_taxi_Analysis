import bokeh, bokeh.models
from bokeh.plotting import figure,show,output_file
# from bokeh.io import output_notebook
# output_notebook()

import geopandas as gpd
from shapely.geometry import Point
# import urllib
# import dask.dataframe as dd
# import dask.distributed
import numpy as np
import pandas as pd
import sys

# import sklearn.preprocessing

# client = dask.distributed.Client()

for hour in range(24):
    output_file("{0}.html".format(hour))
    coord_system = {'init': 'epsg:4326'}
    df = gpd.read_file('/work/via/taxi_zones/taxi_zones_new.shp').to_crs(coord_system)
    df = df.drop(['Shape_Area', 'Shape_Leng', 'OBJECTID'], axis=1)
    df_pickup=pd.read_csv("./pickup_hour_heatmap.csv")
    df_pickup=df_pickup.rename(columns={"zone":"LocationID"})
    df_pickup["LocationID"]=df_pickup["LocationID"].astype(int)
    df0_pickup=df_pickup[df_pickup["hour"]==hour][["LocationID","pickup_count"]]
    pickup_count=pd.merge(left=df, right=df0_pickup)
    pickup_count["pickup_count"]=pickup_count["pickup_count"]+0.1

    print "ploting hour {0}".format(hour)
    print len(pickup_count)

    gjds = bokeh.models.GeoJSONDataSource(geojson=pickup_count.to_json())
    TOOLS = "pan,wheel_zoom,reset,hover,save"

    p = figure(title="NYC Taxi Districts", tools=TOOLS,
        x_axis_location=None, y_axis_location=None, 
        responsive=True)

    color_mapper = bokeh.models.LogColorMapper(palette=bokeh.palettes.Viridis256)

    p.patches('xs', 'ys', 
            fill_color={'field': 'pickup_count', 'transform': color_mapper},
            fill_alpha=1., line_color="black", line_width=0.5,          
            source=gjds)

    p.grid.grid_line_color = None

    hover = p.select_one(bokeh.models.HoverTool)
    hover.point_policy = "follow_mouse"
    hover.tooltips = u"""
    <div> 
        <div class="bokeh_hover_tooltip">Name : @zone</div>
        <div class="bokeh_hover_tooltip">Borough : @borough</div>
        <div class="bokeh_hover_tooltip">Zone ID : @LocationID</div>
        <div class="bokeh_hover_tooltip">(Lon, Lat) : ($x E, $y N)</div>
    </div>
    """

#p.circle([-73.966,], [40.78,], size=10, fill_color='magenta', line_color='yellow', line_width=1, alpha=1.0)
    # show(p)
    # bokeh.io.save(p, "{0}.html".format(hour))
    bokeh.io.export_png(p, "{0}.png".format(hour))
