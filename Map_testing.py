from urllib.request import urlopen
import json as js
import os
import pandas as pd 
import plotly.express as px



def map_trx_data():

    map_trx_path=os.listdir("/workspaces/Phonepe_Pulse_Data/pulse/data/map/transaction/hover/country/india/state/")
    #print(map_trx_path)

    #map_Transaction_Dictionary
    map_trx_data_dict={ 'map_trx_state':[],'map__trx_dist':[],'map_trx_years':[],'map_trx_qtr':[],'map_trx_amount':[],'map_trx_count':[]}

    for map_trx_states in map_trx_path:
        map_trx_years_path=os.listdir("/workspaces/Phonepe_Pulse_Data/pulse/data/map/transaction/hover/country/india/state/"+map_trx_states+"/")
        #print(map_trx_years_path)
        
        for map_trx_years in map_trx_years_path:
            map_trx_file_path=os.listdir("/workspaces/Phonepe_Pulse_Data/pulse/data/map/transaction/hover/country/india/state/"+map_trx_states+"/"+map_trx_years+"/")
            #print(map_trx_file_path) 

            for map_trx_file in map_trx_file_path:
                with open("/workspaces/Phonepe_Pulse_Data/pulse/data/map/transaction/hover/country/india/state/"+map_trx_states+"/"+map_trx_years+"/"+map_trx_file,"r")as map_json_file:
                    map_trx_json_data=js.load(map_json_file)    
                    
                    
                    for i in map_trx_json_data['data']['hoverDataList']:
                        map_trx_dist=[i][0]['name']
                        map_trx_count=[i][0]['metric'][0]['count']
                        map_trx_amt=[i][0]['metric'][0]['amount']
                        map_trx_data_dict['map_trx_state'].append(map_trx_states.replace("-"," "))
                        map_trx_data_dict['map__trx_dist'].append(map_trx_dist.strip("district"))
                        map_trx_data_dict['map_trx_years'].append(int(map_trx_years))
                        map_trx_data_dict['map_trx_qtr'].append(int(map_trx_file.strip(".json")))
                        map_trx_data_dict['map_trx_amount'].append(int(map_trx_amt))
                        map_trx_data_dict['map_trx_count'].append(map_trx_count)


                    #Map_Transaction_Dataframe:
                    map_trx_data_df=pd.DataFrame(map_trx_data_dict)
                    #map_trx_data_df.to_csv("map_trx_data.csv",index="False")
    return map_trx_data_df


#map_trx_data().to_csv("map_transaction.csv")

'''
with urlopen('https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson') as response:
    State = js.load(response)
    #print(State)

import pandas as pd
df = pd.read_csv("/workspaces/Phonepe_Pulse_Data/map_trx_data.csv",
                   dtype={"map_trx_state": str})



df2 = pd.read_csv('India_States.csv')
df['map_trx_state'] = df2['State/UT']

fig = px.choropleth(df, geojson=State, featureidkey='properties.ST_NM', locations='map_trx_state',color='map_trx_state',
                        color_continuous_scale="sunset",#Viridis"
                        hover_data="map_trx_count",
                        hover_name="map_trx_amount",
                        labels={'map_trx_amount':'state-wise_transaction amount'})



#geojson="",
 #                 featureidkey='properties.ST_NM',
 #                 locations='State',
 #                 color='Total_Appopens',
 #                 color_continuous_scale='sunset'


fig.update_geos(fitbounds="locations", visible=False)
fig.show()
'''

def map_user_data():

    #map_User_Dictionary:
    map_user_data_dict={'map_user_state':[],'map_user_dist':[],'map_user_years':[],'map_user_qtr':[],'map_reg_users':[],'map_app_opens':[]}

    map_user_path=os.listdir("/workspaces/Phonepe_Pulse_Data/pulse/data/map/user/hover/country/india/state/")
    #print(map_user_path)

    for map_user_states in map_user_path:
        map_user_years_path=os.listdir("/workspaces/Phonepe_Pulse_Data/pulse/data/map/user/hover/country/india/state/"+map_user_states+"/")
        #print(map_user_years_path)

        for map_user_years in map_user_years_path:
            map_user_file_path=os.listdir("/workspaces/Phonepe_Pulse_Data/pulse/data/map/user/hover/country/india/state/"+map_user_states+"/"+map_user_years+"/")
            #print(map_user_file_path) 

            for map_user_file in map_user_file_path:
                with open("/workspaces/Phonepe_Pulse_Data/pulse/data/map/user/hover/country/india/state/"+map_user_states+"/"+map_user_years+"/"+map_user_file,"r")as map_json_file:
                    map_user_json_data=js.load(map_json_file)  
                    #print(map_user_json_data)
            
                    
                    for i in map_user_json_data['data']['hoverData'].items():                    
                        map_user_data_dict['map_user_dist'].append(i[0].strip("district"))
                        map_user_data_dict['map_reg_users'].append(i[1]['registeredUsers'])
                        map_user_data_dict['map_app_opens'].append(i[1]['appOpens'])
                        map_user_data_dict['map_user_state'].append(map_user_states.replace("-"," "))
                        map_user_data_dict['map_user_years'].append(int(map_user_years))
                        map_user_data_dict['map_user_qtr'].append(int(map_user_file.strip(".json")))


    #Map_User_Dataframe:
        map_user_data_df=pd.DataFrame(map_user_data_dict)

    return map_user_data_df


map_user_df=map_user_data()

with urlopen('https://raw.githubusercontent.com/civictech-India/INDIA-GEO-JSON-Datasets/main/india_states.json') as response:
    State = js.load(response)
    #print(State)


indian_state = pd.read_csv('India_States.csv')
map_user_df['map_user_state'] = indian_state['State/UT']

fig = px.choropleth(map_user_df, geojson=State, featureidkey='properties.NAME_1', locations='map_user_state',color='map_user_state',
                        #color_continuous_scale="sunset",#Viridis"
                        hover_data='map_reg_users',
                        #hover_data="map_app_opens",
                        labels={'map_app_opens':'state-wise_application_opens'})



#geojson="",
 #                 featureidkey='properties.ST_NM',
 #                 locations='State',
 #                 color='Total_Appopens',
 #                 color_continuous_scale='sunset'


fig.update_geos(fitbounds="locations", visible=False)
fig.show()