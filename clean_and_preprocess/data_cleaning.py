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

if __name__ == "__main__":

    df_trip = pd.read_csv("../via_data_challenge/original_data/trip_data/trip_data_8.csv")
    df_fare = pd.read_csv("../via_data_challenge/original_data/trip_fare/trip_fare_8.csv")

    rename_columns(df_trip, df_fare)
    df = merge(df_trip, df_fare)
    drop_wrong_data(df)

    df.to_csv("trip_data_2013_08.csv")

