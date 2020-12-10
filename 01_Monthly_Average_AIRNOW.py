
# Find monthly average for all AirNOW data

from random import random

import os 
import time

import requests as re
import pandas as pd
from io import StringIO
import geopandas as gpd
import numpy as np

import geojson # an extension of JSON with support for geographic data

import matplotlib # base python plotting library
import matplotlib.pyplot as plt # submodule of matplotlib

from pandas.plotting import register_matplotlib_converters
register_matplotlib_converters()

# Take all hourly data
base = "output/air_now/"
folder = base + "raw/by_hour"
files = [x for x in os.listdir(folder) if x.endswith(".csv")]

# Append all data
df_all = pd.DataFrame()
for file1 in files:
    df = pd.read_csv(folder +"/"+ file1)
    df_all = df_all.append(df)

# Format date
df_all.drop_duplicates().reset_index()
df_all['date'] = pd.DatetimeIndex(df_all['date'])
df_all = df_all.sort_values(['date','SiteName'], ascending=[1,1]).reset_index(drop=True)
df_all['day_of_week'] = df_all['date'].dt.dayofweek
df_all['year'] = df_all['date'].dt.year
df_all['month'] = df_all['date'].dt.month

# Replace null values with na
df_all['RawConcentration'] = df_all['RawConcentration'].replace(-999, np.NaN)
df_all['AQI'] = df_all['AQI'].replace(-999, np.NaN)


# Group and aggregate to get mean and number of observations for variables per year/month for every observation
df_grouped = df_all.groupby(['Latitude', 'Longitude','Parameter', 'Unit', 
            'Category', 'SiteName', 'AgencyName','FullAQSCode', 
            'IntlAQSCode', 'year','month']).agg({'Value':['mean', 'count'],
                                        'RawConcentration':['mean', 'count'],
                                        'AQI':['mean', 'count']})
# Flatten hierarchial index
df_grouped.columns = ['_'.join(col).strip('_') for col in df_grouped.columns]
df_grouped = df_grouped.reset_index()
# Make it a geodataframe
gdf_all = gpd.GeoDataFrame(df_grouped, 
                            geometry=gpd.points_from_xy(df_grouped.Longitude, df_grouped.Latitude)).reset_index(drop=True)
gdf_all.set_crs(epsg=4326, inplace=True)
gdf_all.to_file(base + 'airnow_monthly_averages.json')