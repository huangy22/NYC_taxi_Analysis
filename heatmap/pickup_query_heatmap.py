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

if __name__ == "__main__":

    disk_engine = create_engine('sqlite:////work/via/trip_data_2013.db')

    query_pickup = '''SELECT pickup_datetime, pickup_LocationID
                    FROM trip_data'''

    df_pickup = pd.read_sql_query(query_pickup, disk_engine)
    df_pickup["hour"] = df_pickup["pickup_datetime"].apply(lambda x: pd.to_datetime(x).hour)
    
    print df_pickup.head()
    
    agg_pickup = df_pickup.groupby(["hour", "pickup_LocationID"], as_index=False).count()
    agg_pickup.columns = ['hour', 'zone', 'pickup_count']

    agg_pickup.to_csv("pickup_hour_heatmap.csv")

