

import folium 
import requests

import pandas as pd


geo_json_data = requests.get(
    "https://raw.githubusercontent.com/civictech-India/INDIA-GEO-JSON-Datasets/main/india_states.json"
).json()

#print(geo_json_data)

m=folium.map([22,77],zoom_start=4)

folium.GeoJson(geo_json_data).add_to(m)

map_trx_data = pd.read_csv('/workspaces/Phonepe_Pulse_Data/map_trx_data.csv')


#from branca.colormap import linear
#colormap = linear.YlGn_09.scale(map_trx_data.map_trx_count.min(), map_trx_data.map_trx_count.max())
#print(colormap(5.0))

