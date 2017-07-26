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

    query_dropoff = '''SELECT dropoff_datetime, dropoff_LocationID
                    FROM trip_data'''

    df_dropoff = pd.read_sql_query(query_dropoff, disk_engine)
    df_dropoff["hour"] = df_dropoff["dropoff_datetime"].apply(lambda x: pd.to_datetime(x).hour)
    
    print df_dropoff.head()
    
    agg_dropoff = df_dropoff.groupby(["hour", "dropoff_LocationID"], as_index=False).count()
    agg_dropoff.columns = ['hour', 'zone', 'dropoff_count']

    agg_dropoff.to_csv("dropoff_hour_heatmap.csv")

