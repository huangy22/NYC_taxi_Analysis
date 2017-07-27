#!/home/kun/anaconda2/bin python
import sys
import numpy as np
import pandas as pd

for i in range(0, 13):
    df = pd.read_csv('UES_recommend_next_rides_{0}.csv'.format(i), header=0, low_memory=False)
    print i, len(df)
    print df.describe()

    if i == 0:
        with open('UES_recommend_next_rides_total.csv', 'w') as f:
            df.to_csv(f)
    else:
        with open('UES_recommend_next_rides_total.csv', 'a') as f:
            df.to_csv(f, header=False)

