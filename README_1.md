# Via_NYC_taxi

A data analysis 

This project is using the NYC taxi data from the period before July 2016 (described and available here: http://www.nyc.gov/html/tlc/html/about/trip_record_data.shtml, also available either through BiqQuery https://bigquery.cloud.google.com/table/imjasonh-storage:nyctaxi.trip_data, or in smaller samples from http://www.andresmh.com/nyctaxitrips/).

I suggest that you use docker to install all the required packages for this project to run.
* Step 1: follow the instruction on https://docs.docker.com/engine/installation/ to install docker.
* Step 2: Go to the directory ```./docker/```, run the command:
``` ./build.sh ```
* Step 3: Run the command ```./start.sh``` to open the jupyter notebook in your browser.

