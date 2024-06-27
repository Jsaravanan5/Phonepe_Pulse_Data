

#Map data >> Transaction

import os
import json as js
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

map_trx_path=os.listdir("/workspaces/Phonepe_Pulse_Data/pulse/data/map/transaction/hover/country/india/state/")
#print(map_trx_path)

#map_Transaction_Dictionary
map_trx_data={ 'map_trx_state':[],'map__trx_dist':[],'map_trx_years':[],'map_trx_qtr':[],'map_trx_amount':[],'map_trx_count':[]}

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
                    map_trx_data['map_trx_state'].append(map_trx_states)
                    map_trx_data['map__trx_dist'].append(map_trx_dist)
                    map_trx_data['map_trx_years'].append(map_trx_years)
                    map_trx_data['map_trx_qtr'].append(int(map_trx_file.strip(".json")))
                    map_trx_data['map_trx_amount'].append(map_trx_amt)
                    #print(map_trx_data['map_trx_amount'])
                    map_trx_data['map_trx_count'].append(map_trx_count)


#Map_Transaction_Dataframe:
map_trx_data_df=pd.DataFrame(map_trx_data)
#print(map_trx_data_df)

# Map User Data:

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
                print(map_user_json_data)

                #for i in map_user_json_data:
                    #print(i)
                    #map_user_dist=i['data']['hoverData']
                    #print(map_user_dist)






                    





                    