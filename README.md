# Via_NYC_taxi


This project is using the NYC taxi data from the period before July 2016 (described and available here: http://www.nyc.gov/html/tlc/html/about/trip_record_data.shtml, also available either through BiqQuery https://bigquery.cloud.google.com/table/imjasonh-storage:nyctaxi.trip_data, or in smaller samples from http://www.andresmh.com/nyctaxitrips/).

I suggest that you use docker to install all the required packages for this project to run.
* Step 1: follow the instruction on https://docs.docker.com/engine/installation/ to install docker.
* Step 2: Go to the directory ```./docker/```, run the command:
``` ./build.sh ```
* Step 3: Run the command ```./start.sh``` to open the jupyter notebook in your browser.

The pipeline to run the whole project is as follows:

* Step 1: Go to http://www.andresmh.com/nyctaxitrips/ to download the two files ```trip_data.7z``` and ```trip_fare.7z```, extract the files in a new folder in the working directory ```./original_data```. These two files include all the taxi ride data during 2013 from January to December.
* Step 2: Go to folder ```./cleaned_data``` to run the command:

  ```python data_clean.py```
* Step 3: Go back to the working directory, run the preprocess code to extract the locationID for all the dropoff and pickup locations of each ride. There are two versions of code ```preprocess_add_locationID.py``` and the parallel version ```preprocess_add_locationID_parallel.py```. Run the code with a input variable (integer from 1 to 12) to process the data set of the corresponding month, for example, to process data for August 2013, run the following command: 
  
  ``` python preprocess_add_locationID.py 8``` 
  
  This code may take several hours to run.

* Step 4: Go to folder ```./wait_time``` and run the command ```./submit.sh``` to submit multiple jobs to calculate the driver's waiting time for each ride that drops off passengers in the areas of our interest during a given month.
  
* Step 5: Go to folder ```./recommend_next_ride``` and run the code ```./recommend_next.py``` to get the recommended next pickup for drivers.

* Step 6: Given all the calculated results, finally we can open our jupyter notebook to run the data analysis and see the results and graphs!



