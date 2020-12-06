# Pull EPA data from API for CA
# Save in folder

from random import random

import os 
import time

import requests as re
import pandas as pd
from io import StringIO
import geopandas as gpd
import numpy as np
from scipy import fft, ifft, signal

import geojson # an extension of JSON with support for geographic data

import matplotlib # base python plotting library
import matplotlib.pyplot as plt # submodule of matplotlib

from pandas.plotting import register_matplotlib_converters
register_matplotlib_converters()

###############
#### SETUP ####
###############
options = {}
options["url"] = "http://www.airnowapi.org/aq/data/"
options["start_hour_utc"] = "00"
options["end_hour_utc"] = "23"
options["parameters"] = "OZONE,PM25,PM10,CO,NO2,SO2"

### BOUNDING BOX NEEDS TO BE SMALL TO NOT HIT UPPER LIMIT
# tracts_gdf = gpd.read_file("zip://../indata/census/Tracts/cb_2018_06_tract_500k.zip")
# tracts_gdf = tracts_gdf.to_crs('epsg:4326')
# xmin, ymin, xmax, ymax = tracts_gdf.total_bounds
# # Divide into increments of
# inc = 10
# min_list = [ymin + (ymax-ymin)/10*i for i in range(inc)]
# max_list = [ymin + (ymax-ymin)/10*(i+1) for i in range(inc)]

# Find all monitor locations
REQUEST_URL =  'http://www.airnowapi.org/aq/data/?startDate=2020-05-28T17&endDate=2020-05-28T18&parameters=OZONE,PM25,PM10,CO,NO2,SO2&BBOX=-124.590836,32.683014,-114.307632,42.022513&dataType=A&format=application/json&verbose=1&nowcastonly=0&includerawconcentrations=0&API_KEY=C0A6EC3C-BBB2-49DD-92EF-0A9D67999AD0'
# Perform the AirNow API data request
r= re.get(REQUEST_URL)
x = r.json()
df = pd.DataFrame(x)
gdf = gpd.GeoDataFrame(df, geometry=gpd.points_from_xy(df.Longitude, df.Latitude))
# Limit to CA boundaries
tracts_gdf = gpd.read_file("zip://data/census/Tracts/cb_2018_06_tract_500k.zip")
tracts_gdf = tracts_gdf.to_crs('epsg:4326')
intersections_gdf = gpd.sjoin(gdf,tracts_gdf)

fig, ax= plt.subplots()
tracts_gdf.plot(ax=ax, color='lightgrey') 
intersections_gdf.plot(ax=ax, color='purple')
intersections_gdf = intersections_gdf[['SiteName','AgencyName', 'FullAQSCode', 'IntlAQSCode', 'geometry']].copy()
intersections_gdf = intersections_gdf.drop_duplicates().reset_index()
intersections_gdf.to_file("output/air_now/airnow_ca_sensors.json", driver="GeoJSON")

options["data_type"] = "B"
options["format"] = "application/json"
options["ext"] = "csv"
options["api_key"] = "C0A6EC3C-BBB2-49DD-92EF-0A9D67999AD0"

##################
#### API PULL ####
##################

import os
import sys
from datetime import datetime
from os.path import expanduser
import urllib
import calendar
from datetime import datetime
from pytz import timezone


# Loop through dates
counting = 0 

# Loop through years
for i in range(2016,2017):
# Loop through months
    print(i)
    df_all = pd.DataFrame()
    
    for j in range(12):
        if i==2016 and j==0:
            continue

        start_month = str(j+1).zfill(2)
        end_month = str(j+1).zfill(2)

        end_month_day = str(calendar.monthrange(i,j+1)[1]).zfill(2)

        options["start_date"] = str(i) + "-" + start_month + "-01"
        options["end_date"] = str(i) + "-" + end_month + "-" + end_month_day

        start_month = str(j+1).zfill(2)
        end_month = str(j+1).zfill(2)
        end_month_day = str(calendar.monthrange(i,j+1)[1]).zfill(2)

        if j==0:
            sy_calib = i-1
            sm_calib = 12
        else:
            sy_calib = i
            sm_calib= j
        if j==11:
            em_calib = 1
            ey_calib = i+1
        else:
            em_calib = j+2
            ey_calib = i

        sm_calib_day = str(calendar.monthrange(sy_calib,sm_calib)[1]).zfill(2)
        em_calib_day = str(calendar.monthrange(ey_calib,em_calib)[1]).zfill(2)

        sd_calib = str(sy_calib) + "-" + str(sm_calib).zfill(2) + "-" + sm_calib_day
        ed_calib = str(ey_calib) + "-" + str(em_calib).zfill(2) + "-01"

        print("Requesting AirNowAPI data for " + options["start_date"] + " through " + options["end_date"])
        
        for k in range(len(intersections_gdf)):
            # options['bbox'] = str(xmin)  + "," + str(min_list[k]) + ","   + str(xmax)  + ","  + str(max_list[k])
            options['bbox'] = ",".join((str(round(intersections_gdf['geometry'][k].x - .01,6)),
                                        str(round(intersections_gdf['geometry'][k].y - .01,6)),
                                        str(round(intersections_gdf['geometry'][k].x + .01,6)),
                                        str(round(intersections_gdf['geometry'][k].y + .01,6))))

            REQUEST_URL = options["url"] \
                    + "?startdate=" + options["start_date"] \
                    + "t" + options["start_hour_utc"] \
                    + "&enddate=" + options["end_date"] \
                    + "t" + options["end_hour_utc"] \
                    + "&parameters=" + options["parameters"] \
                    + "&bbox=" + options["bbox"] \
                    + "&datatype=" + options["data_type"] \
                    + "&format=" + options["format"] \
                    + "&verbose=1&nowcastonly=0&includerawconcentrations=1" \
                    + "&api_key=" + options["api_key"]

            # Request AirNowAPI data
            # print("Requesting AirNowAPI data for " + options["start_date"] + " through " + options["end_date"])

            # User's home directory.
            home_dir = "/Users/hikarimurayama/Documents/repos/CA_ag_pollution/output/air_now/raw"
            download_file_name = options["start_date"] + "_" + options["end_date"] + "." + options["ext"]
            download_file = os.path.join(home_dir, download_file_name)

            # Perform the AirNow API data request
            r= re.get(REQUEST_URL)
            x = r.json()
            df = pd.DataFrame(x)

            if df.empty:
                print("No data for" + intersections_gdf.iloc[k]['SiteName'] )
                counting +=1

                if counting>0 and counting%490 == 0:
                    print("rest")
                    time.sleep(3610)
                
                continue 

            # Reduce file size and take average daily vbalues
            df['UTC'] = df['UTC'].str.replace('T',' ')
            # Convert to date time
            df['UTC'] = pd.to_datetime(df['UTC'])
            # Change UTC time to Pacific time zone
            df['UTC'] = df['UTC'].dt.tz_localize('UTC').dt.tz_convert('US/Pacific')
            # Find day
            df['date'] = df['UTC'].dt.date
            # Keep if within date range
            start = pd.to_datetime(options["start_date"]+' 00:00').tz_localize('US/Pacific')
            end = pd.to_datetime(ed_calib+' 00:00').tz_localize('US/Pacific')
            df = df.loc[(df['UTC']>=start) & (df['UTC']<end)]

            df_all = df_all.append(df)

            # Every 500th pull rest for an hour (rate limiting DO NOT ERASE)
            if counting>0 and counting%490 == 0:
                print("rest")
                time.sleep(3610)
            counting+=1


        df_all.to_csv(home_dir + '/by_hour/' + download_file_name) 

        # Replace null values with na
        df_all['RawConcentration'] = df_all['RawConcentration'].replace(-999, np.NaN)
        df_all['AQI'] = df_all['AQI'].replace(-999, np.NaN)

        # Group and aggregate to get mean and number of observations for variables per day
        df_grouped = df_all.groupby(['Latitude', 'Longitude','Parameter', 'Unit', 
                    'Category', 'SiteName', 'AgencyName','FullAQSCode', 
                    'IntlAQSCode', 'date']).agg({'Value':['mean', 'count'],
                                                'RawConcentration':['mean', 'count'],
                                                'AQI':['mean', 'count']})
        # Flatten hierarchial index
        df_grouped.columns = ['_'.join(col).strip('_') for col in df_grouped.columns]

        df_grouped.to_csv(download_file) 

        # Download complete
        print("Download URL: %s" % REQUEST_URL)
        print("Download File: %s" % download_file)


