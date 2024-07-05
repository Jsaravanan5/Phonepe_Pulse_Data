

#Map data >> Transaction

import os
import json as js
import pandas as pd
import matplotlib.pyplot as plt
import polars as pl
import mysql.connector
import streamlit as st
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

#print(map_trx_data())
#18296 rows


# Map User Data:

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
        map_user_data_df=pl.DataFrame(map_user_data_dict)

    return map_user_data_df

#18300 rows    
#print(map_user_data())








#Map_insurance:

def map_ins_data():

    #map Insurance Dictionary:
    map_ins_data_dict={'map_ins_state':[],'map_ins_dist':[],'map_ins_years':[],'map_ins_qtr':[],'map_ins_amount':[],'map_ins_count':[] }

    map_ins_path=os.listdir("/workspaces/Phonepe_Pulse_Data/pulse/data/map/insurance/hover/country/india/state/")
    #print(map_ins_path)

    for map_ins_states in map_ins_path:
        map_ins_years_path=os.listdir("/workspaces/Phonepe_Pulse_Data/pulse/data/map/insurance/hover/country/india/state/"+map_ins_states+"/")
        #print(map_ins_years_path)

        for map_ins_years in map_ins_years_path:
            map_ins_file_path=os.listdir("/workspaces/Phonepe_Pulse_Data/pulse/data/map/insurance/hover/country/india/state/"+map_ins_states+"/"+map_ins_years+"/")
            #print(map_ins_file_path) 

            for map_ins_file in map_ins_file_path:
                with open("/workspaces/Phonepe_Pulse_Data/pulse/data/map/insurance/hover/country/india/state/"+map_ins_states+"/"+map_ins_years+"/"+map_ins_file,"r")as map_json_file:
                    map_ins_json_data=js.load(map_json_file)  
                

                for i in map_ins_json_data['data']['hoverDataList']:
                    map_ins_dist=[i][0]['name']
                    map_ins_count=[i][0]['metric'][0]['count']
                    map_ins_amt=[i][0]['metric'][0]['amount']
                    map_ins_data_dict['map_ins_state'].append(map_ins_states.replace("-"," "))
                    map_ins_data_dict['map_ins_dist'].append(map_ins_dist.strip("district"))
                    map_ins_data_dict['map_ins_years'].append(int(map_ins_years))
                    map_ins_data_dict['map_ins_qtr'].append(int(map_ins_file.strip(".json")))
                    map_ins_data_dict['map_ins_amount'].append(int(map_ins_amt))
                    map_ins_data_dict['map_ins_count'].append(map_ins_count)



    #Map_insurance_dataframe:

    map_ins_data_df=pd.DataFrame(map_ins_data_dict)
    #map_ins_data_df.to_csv('map_ins_data.csv',index='False')
    #print(map_ins_data_df)
    return map_ins_data_df

#11559 rows
#print(map_ins_data())




           






                    





                    