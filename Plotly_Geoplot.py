

#Map data >> Transaction

import os
import json as js
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

map_trx_path=os.listdir("/workspaces/Phonepe_Pulse_Data/pulse/data/map/transaction/hover/country/india/state/")
#print(map_trx_path)

for map_trx_states in map_trx_path:
    map_trx_years_path=os.listdir("/workspaces/Phonepe_Pulse_Data/pulse/data/map/transaction/hover/country/india/state/"+map_trx_states+"/")
    #print(map_trx_years_path)
    
    for map_trx_years in map_trx_years_path:
        map_trx_file_path=os.listdir("/workspaces/Phonepe_Pulse_Data/pulse/data/map/transaction/hover/country/india/state/"+map_trx_states+"/"+map_trx_years+"/")
        #print(map_trx_file_path) 

        for map_trx_file in map_trx_file_path:
            with open("/workspaces/Phonepe_Pulse_Data/pulse/data/map/transaction/hover/country/india/state/"+map_trx_states+"/"+map_trx_years+"/"+map_trx_file,"r")as map_json_file:
                map_trx_json_data=js.load(map_json_file)    
                #print(map_trx_json_data)
                map_dist=[]
                for i in map_trx_json_data['data']['hoverDataList']:   #[0]['metric']:
                    map_trx_dist=[i][0]['name']
                    #print(map_trx_dist)
                    map_trx_count=[i][0]['metric'][0]['count']
                    print(map_trx_count)
                    map_trx_amount=[i][0]['metric'][0]['amount']
                    map_trx_amt=round(map_trx_amount,2)
                    print(map_trx_amount)





                    