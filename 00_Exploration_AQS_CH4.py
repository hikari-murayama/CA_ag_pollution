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

# Define functions
def get_json(url):
    r = re.get(url)
    x = r.json()
    print(x.keys())
    df = pd.DataFrame(x[list(x.keys())[-1]])
    
    return df

# Parameters
email = "hikki.m@gmail.com"
key = "coppercrane82"
state = "06" #CA
county = "001" #Alameda County

# NOx, Ozone, SO2, Lead(multiple options picked one)
# params = ["42603" , "44201" , "42401", "12128"]
params = ['43201']


##################
#### API PULL ####
##################


# Grab parameter class list
list_query = "https://aqs.epa.gov/data/api/list/classes?email=" + email + "&key=" + key
print(list_query)
df_class = get_json(list_query)
df_class.to_csv("output/epa_aqs/classes.csv") 
# Wait 5 seconds
time.sleep(5)

# Grab parameters per class list
param_query = ''.join(["https://aqs.epa.gov/data/api/list/parametersByClass?email=",
                      email, 
                      "&key=", 
                      key, 
                       "&pc=", 
                       "ALL"])
df_parameters = get_json(param_query)
df_parameters.to_csv("output/epa_aqs/parameters.csv") 

# Wait 5 seconds
time.sleep(5)

# Grab sites by county
sites_by_county = ''.join(["https://aqs.epa.gov/data/api/list/sitesByCounty?email=",
                         email,
                         "&key=",
                         key,
                         "&state=",
                         state,
                         "&county=",
                         county
                          ])
df_sites = get_json(sites_by_county)
df_sites.to_csv("output/epa_aqs/sites_for_AC.csv") 

# Wait 5 seconds
time.sleep(5)

# Grab data for Alameda County, loop over designated parameters and years
for param in params: 
    str_param = str(param)

    # Create directory if it doesn't exist
    dirName = "output/epa_aqs/" + param + "/"
    if not os.path.exists(dirName):
        os.mkdir(dirName)
        print("Directory " , dirName ,  " Created ")
    else:    
        print("Directory " , dirName ,  " already exists")

    # Loop through years
    for yr in range(2016,2020):
        str_yr = str(yr)
        bdate = str_yr + "0101"
        edate = str_yr + "1231"

        ca_query = ''.join(["https://aqs.epa.gov/data/api/dailyData/byState?email=",
                    email ,
                    "&key=",
                    key,
                    "&param=",
                    str_param,
                    "&bdate=",
                    bdate,
                    "&edate=",
                    edate,
                    "&state=",
                    state])
        df_ca = get_json(ca_query)

        # If retrieved json is empty, skip
        if len(df_ca) == 0:
            print(str_param,str_yr,"no data")
            pass
        else:

            # Format date/time and sort by site and date
            df_ca['date_local']= pd.to_datetime(df_ca['date_local']) 
            df_ca['date_local'].describe()
            df_ca = df_ca.sort_values(by=['site_number', 'date_local'],ascending=True)

            # Save it out as csv
            df_ca.to_csv(''.join(["output/epa_aqs/",
                            str_param,
                            "/",
                            str_param,
                            "_",
                            str_yr, 
                            ".csv"]))

            # Create preliminary graph for data over the year
            print("Sites:",df_ca['site_number'].unique())
            fig, ax = plt.subplots(figsize=(10,10))
            for sn in df_ca['site_number'].unique():
                ax.plot(df_ca.loc[df_ca['site_number']==sn]['date_local'],
                        df_ca.loc[df_ca['site_number']==sn]['arithmetic_mean'],
                        label=sn)

            plt.legend()
            plt.xticks(rotation='vertical')
            plt.title(str_param + "_" + str_yr)
            plt.savefig(''.join(["output/epa_aqs/raw/",
                            str_param,
                            "/",
                            str_param,
                            "_",
                            str_yr, 
                            ".png"]))
        # Wait 5 seconds
        time.sleep(5)
