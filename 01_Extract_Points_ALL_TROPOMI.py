import os
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
import numpy.ma as ma
import pandas as pd
import rasterio as rio
from rasterio.plot import plotting_extent
import geopandas as gpd

# Rasterstats contains the zonalstatistics function 
# that you will use to extract raster values
import rasterstats as rs
import earthpy as et
import earthpy.plot as ep

import zipfile

from functools import reduce

# Set consistent plotting style
sns.set_style("white")
sns.set(font_scale=1.5)

path = 'data/NACP_Vista_CA_CH4_Inventory_1726/data'

def listdirs(path):
    return [d for d in os.listdir(path) if os.path.isdir(os.path.join(path, d))]


# # Read in vista points and separate by month
# alldafiles = listdirs('data/NACP_Vista_CA_CH4_Inventory_1726/data')
# imported_files = [gpd.read_file(path + '/' + i + '/' + i + '.shp') for i in alldafiles]
# gdf = gpd.GeoDataFrame(pd.concat(imported_files, ignore_index=True), crs=imported_files[0].crs)

# for subdir in listdirs('data/NACP_Vista_CA_CH4_Inventory_1726/data'):
#     points_gdf = gpd.read_file(path + '/' + subdir + '/' + subdir + '.shp')
    
#     # Create 20m buffer
#     poly_gdf = points_gdf.copy()
#     poly_gdf["geometry"] =points_gdf.geometry.buffer(.1)



def extract_tropomi(pollutant, pollutant_tif):
    # Load & plot the data
    pollutant_path = os.path.join("data", "google_earth_engine", pollutant_tif)

    pollutant_data = []
    for i in range(12):
        with rio.open(pollutant_path) as src:
            # Masked = True sets no data values to np.nan if they are in the metadata
            pollutant_data.append(src.read(i+1, masked=True))
            pollutant_meta = src.profile


    for subdir in listdirs('data/NACP_Vista_CA_CH4_Inventory_1726/data'):
        if subdir != 'Vista_CA_Oil_and_Gas_Wells':
            continue

        points_gdf = gpd.read_file(path + '/' + subdir + '/' + subdir + '.shp')
        
        # Create 20m buffer
        poly_gdf = points_gdf.copy()
        poly_gdf["geometry"] =points_gdf.geometry.buffer(.5)
        
        # Extract zonal stats
        zonal_stats_df = []
        for i in range(12):
            zonal_stats = rs.zonal_stats(poly_gdf,
                                            pollutant_data[i],
                                            nodata=-999,
                                            affine= pollutant_meta['transform'],
                                            geojson_out=True,
                                            copy_properties=True,
                                            stats="count mean")
            df = gpd.GeoDataFrame.from_features(zonal_stats)

            df.rename(columns={'count':'count_'+ str(i+1).zfill(2), 
                                'mean':'mean_'+ str(i+1).zfill(2)}, inplace=True)
            zonal_stats_df.append(df)
        
        on_list = list(zonal_stats_df[0].columns)[:-2]
        gdf = gpd.GeoDataFrame(reduce(lambda x, y: pd.merge(x, y, on = on_list), zonal_stats_df), crs=zonal_stats_df[0].crs)

        gdf.to_file('output/' + pollutant + '_points/' + subdir +'.json', driver='GeoJSON')

        gdf= gpd.GeoDataFrame()



pollutant_tifs = ['CH4_2019.tif','O3_2019.tif','SO2_2019.tif','NO2_2019.tif']
pollutants = ['CH4','O3','SO2','NO2']
extract_tropomi(pollutants[3], pollutant_tifs[3]) 
for i in range(len(pollutants)):
   extract_tropomi(pollutants[i], pollutant_tifs[i]) 