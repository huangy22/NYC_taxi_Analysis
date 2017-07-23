#!/home/kun/anaconda2/bin python
import sys
import numpy as np
import pandas as pd
import datetime as dt
from sqlalchemy import create_engine # database connection

disk_engine = create_engine('sqlite:///trip_data_2013_08.db')
for i in range(1, 12):
    df = pd.read_csv(str(i)+'_zone_trip_2013_08.csv', header=0, low_memory=False)
    print len(df)
    print df.describe()
    df.to_sql("trip_data", disk_engine, if_exists='append')
