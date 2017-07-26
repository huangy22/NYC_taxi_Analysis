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
coord_system = {'init': 'epsg:4326'}
df = gpd.read_file('/work/via/taxi_zones/taxi_zones_new.shp').to_crs(coord_system)
df = df.drop(['Shape_Area', 'Shape_Leng', 'OBJECTID'], axis=1)
df_pickup=pd.read_csv("./pickup_hour_heatmap.csv")
df_pickup=df_pickup.rename(columns={"zone":"LocationID"})
df_pickup["LocationID"]=df_pickup["LocationID"].astype(int)
pickup_count=df

for hour in range(24):
    df0_pickup=df_pickup[df_pickup["hour"]==hour][["LocationID","pickup_count"]].copy()
    df0_pickup=df0_pickup.rename(columns={"pickup_count":"pickup_count{0}".format(hour)})
    pickup_count=pd.merge(left=pickup_count, right=df0_pickup,how="outer")
#drop Brooklyn, Staten Island, EWR, boroughs to make visualization more responsive
pickup_count=pickup_count[(pickup_count["borough"]!="Staten Island")&(pickup_count["borough"]!="EWR")]
                          # &(pickup_count["borough"]!="Brooklyn")]
#drop islands to make visualization more responsive
pickup_count=pickup_count[pickup_count["LocationID"].isin([27,2,30,86,117,201,46,184,103])==False]
pickup_count=pickup_count.fillna(0.0) 
print pickup_count.head()

gjds = bokeh.models.GeoJSONDataSource(geojson=pickup_count.to_json())
# TOOLS = "pan,wheel_zoom,reset,hover,save"
TOOLS = ""

p = figure(tools=TOOLS,
    x_axis_location=None, y_axis_location=None, 
    responsive=True, width=500, height=600)

# p = figure(x_axis_location=None, y_axis_location=None, 
    # responsive=True, width=2000, height=2400)

color_mapper = bokeh.models.LogColorMapper(palette=bokeh.palettes.Viridis256, low=10, high=20000)
color_bar = bokeh.models.ColorBar(color_mapper=color_mapper, ticker=bokeh.models.LogTicker(),
                    label_standoff=12, border_line_color=None, location=(0,0))
p.add_layout(color_bar, 'right')

for hour in range(24):
    output_file("{0}.html".format(hour))
    print "ploting hour {0}".format(hour)
    print len(pickup_count)
    p.patches('xs', 'ys', 
            fill_color={'field': 'pickup_count{0}'.format(hour), 'transform': color_mapper},
            fill_alpha=1., line_color="black", line_width=0.5,          
            source=gjds)

    p.grid.grid_line_color = None
    time="0"+str(hour)
    p.title.text="Hour of the Day {0}:00".format(time[-2:])
    p.title.align="center"
    p.title.text_font_size="32px"

    # hover = p.select_one(bokeh.models.HoverTool)
    # hover.point_policy = "follow_mouse"

    # hover.tooltips = u"""
        # <div> 
            # <div class="bokeh_hover_tooltip">Name : @zone</div>
            # <div class="bokeh_hover_tooltip">Borough : @borough</div>
            # <div class="bokeh_hover_tooltip">Zone ID : @LocationID</div>
            # <div class="bokeh_hover_tooltip">(Lon, Lat) : ($x E, $y N)</div>
            # <div class="bokeh_hover_tooltip">pickup_count : @pickup_count{0}</div>
        # </div>
        # """.format(hour)

#p.circle([-73.966,], [40.78,], size=10, fill_color='magenta', line_color='yellow', line_width=1, alpha=1.0)
    # show(p)
    name=("0"+str(hour))
    bokeh.io.save(p, "pickup_{0}.html".format(name[-2:]))
    # bokeh.io.export_png(p, "{0}.png".format(hour))
