import os
import json as js
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import polars as pl
import plotly.express as px


# Function to get Aggregated_insurance:
def agg_ins_data():
    
    # Insurance_dictionary to capture the information:
    Insurance_agg_data={ "Ins_States":[],"Ins_Years":[],"Ins_Qtr":[],"Ins_txn_type":[],"Ins_txn_count":[],"Ins_txn_amount":[] 
                    } 

    ins_states_path=os.listdir("/workspaces/Phonepe_Pulse_Data/pulse/data/aggregated/insurance/country/india/state/")
    agg_ins_path=list(ins_states_path)
    #print(agg_ins_path)

    for ins_states in agg_ins_path:
        agg_ins_years_path=os.listdir("/workspaces/Phonepe_Pulse_Data/pulse/data/aggregated/insurance/country/india/state/"+ins_states+"/")
        #print(ins_years_path)

        for ins_years in agg_ins_years_path:
            agg_ins_file_path=os.listdir("/workspaces/Phonepe_Pulse_Data/pulse/data/aggregated/insurance/country/india/state/"+ins_states+"/"+ins_years+"/")
            #print(agg_ins_file_path)

            for file in agg_ins_file_path:
                with open("/workspaces/Phonepe_Pulse_Data/pulse/data/aggregated/insurance/country/india/state/"+ins_states+"/"+ins_years+"/"+file,"r") as json_file :
                    agg_json_ins_file=js.load(json_file)
                    
                
                for data in agg_json_ins_file['data']['transactionData']:
                    agg_ins_txn_type=data['name']
                    agg_ins_txn_count=data['paymentInstruments'][0]['count']
                    agg_ins_txn_amount=data['paymentInstruments'][0]['amount']
                    Insurance_agg_data['Ins_States'].append(ins_states.replace("-"," "))
                    Insurance_agg_data['Ins_Years'].append(ins_years)
                    Insurance_agg_data['Ins_Qtr'].append(int(file.strip(".json")))
                    Insurance_agg_data['Ins_txn_type'].append(agg_ins_txn_type)
                    Insurance_agg_data['Ins_txn_count'].append(agg_ins_txn_count)
                    Insurance_agg_data['Ins_txn_amount'].append(agg_ins_txn_amount)

         
                    Agg_Insurance_df=pl.DataFrame(Insurance_agg_data)
                                    
    return Agg_Insurance_df

#insurance Dataframe   
#574
#print(agg_ins_data())

    


def agg_user_data():
    #Agg_User_Data_Dictionary:
    user_agg_data= {
                     "User_Ins_States":[],
                     "User_Ins_Years":[],
                     "User_Ins_Qtr":[],
                     "User_Device_Brand":[],
                     "User_Device_count":[],
                     "User_Device_percent":[]
                    }       

    user_states_path=os.listdir("/workspaces/Phonepe_Pulse_Data/pulse/data/aggregated/user/country/india/state/")
    agg_user_path=list(user_states_path)

    #print(agg_user_path)

    for user_states in agg_user_path:
        agg_user_years_path=os.listdir("/workspaces/Phonepe_Pulse_Data/pulse/data/aggregated/user/country/india/state/"+user_states+"/")
        #print(agg_user_years_path)

        for user_years in agg_user_years_path:
            agg_user_file_path=os.listdir("/workspaces/Phonepe_Pulse_Data/pulse/data/aggregated/user/country/india/state/"+user_states+"/"+user_years+"/")
            #print(agg_user_file_path)

            for file in agg_user_file_path:
                with open("/workspaces/Phonepe_Pulse_Data/pulse/data/aggregated/user/country/india/state/"+user_states+"/"+user_years+"/"+file,"r") as json_file :
                    agg_json_user_file=js.load(json_file)

                data=agg_json_user_file['data']['usersByDevice']
                if data is not None:
                    for i in data: 
                        agg_user_device_brand=i['brand']
                        agg_user_device_count=i['count']
                        agg_user_device_percent=i['percentage']
                        user_agg_data["User_Device_Brand"].append(agg_user_device_brand)
                        user_agg_data["User_Device_count"].append(agg_user_device_count)
                        user_agg_data["User_Device_percent"].append(float(agg_user_device_percent*100))
                        user_agg_data['User_Ins_States'].append(user_states.replace("-"," "))
                        user_agg_data["User_Ins_Years"].append(user_years)
                        user_agg_data["User_Ins_Qtr"].append(int(file.strip(".json")))


    #User DataFrame                
    Agg_Users_df=pl.DataFrame(user_agg_data) 
    return Agg_Users_df 




# MAP DATA

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
                        map_trx_data_dict['map_trx_years'].append(map_trx_years)
                        map_trx_data_dict['map_trx_qtr'].append(int(map_trx_file.strip(".json")))
                        map_trx_data_dict['map_trx_amount'].append(map_trx_amt)
                        #print(map_trx_data['map_trx_amount'])
                        map_trx_data_dict['map_trx_count'].append(map_trx_count)


                    #Map_Transaction_Dataframe:
                    map_trx_data_df=pd.DataFrame(map_trx_data_dict)
                    map_trx_data_df.to_csv("map_trx_data.csv",index="False")
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
                        map_user_data_dict['map_user_years'].append(map_user_years)
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
                    map_ins_data_dict['map_ins_years'].append(map_ins_years)
                    map_ins_data_dict['map_ins_qtr'].append(int(map_ins_file.strip(".json")))
                    map_ins_data_dict['map_ins_amount'].append(map_ins_amt)
                    #print(map_trx_data['map_trx_amount'])
                    map_ins_data_dict['map_ins_count'].append(map_ins_count)



    #Map_insurance_dataframe:

    map_ins_data_df=pd.DataFrame(map_ins_data_dict)
    #map_ins_data_df.to_csv('map_ins_data.csv',index='False')
    #print(map_ins_data_df)
    return map_ins_data_df

#11559 rows
#print(map_ins_data())





# TOP DATA


def top_trx_data():

    top_trx_path=os.listdir("/workspaces/Phonepe_Pulse_Data/pulse/data/top/transaction/country/india/state/")
    #print(top_trx_path)

    #top_transaction_Dictionary:
    top_trx_data_dist_dict={'Top_trx_years':[],'Top_trx_qtr':[],'Top_trx_states':[],'Top_trx_dist':[],'top_trx_dist_count':[],'top_trx_dist_amount':[] }

    top_trx_data_pincode_dict={'Top_trx_years':[],'Top_trx_qtr':[],'Top_trx_states':[],'Top_trx_pincodes':[],'top_trx_pincodes_count':[],'top_trx_pincodes_amount':[]}

    for top_trx_states in top_trx_path:
        top_trx_years_path=os.listdir("/workspaces/Phonepe_Pulse_Data/pulse/data/top/transaction/country/india/state"+"/"+top_trx_states+"/")
        #print(top_trx_years_path)

        for top_trx_years in top_trx_years_path:
            top_trx_file_path=os.listdir("/workspaces/Phonepe_Pulse_Data/pulse/data/top/transaction/country/india/state"+"/"+top_trx_states+"/"+top_trx_years+"/")
            #print(top_trx_file_path)

            for top_trx_files in top_trx_file_path:
                with open("/workspaces/Phonepe_Pulse_Data/pulse/data/top/transaction/country/india/state"+"/"+top_trx_states+"/"+top_trx_years+"/"+top_trx_files,"r")as top_trx_file:
                    top_json_file_data=js.load(top_trx_file)   
                    #print(top_json_file_data)  

                    # district  
                    for i in top_json_file_data['data']['districts']:
                        top_trx_data_dist_dict['Top_trx_dist'].append(i['entityName'])
                        top_trx_data_dist_dict['top_trx_dist_count'].append(i['metric']['count'])
                        top_trx_dist_amt=i['metric']['amount']
                        top_trx_data_dist_dict['top_trx_dist_amount'].append(round(top_trx_dist_amt,2))  #round-off
                        top_trx_data_dist_dict['Top_trx_years'].append(top_trx_years)
                        top_trx_data_dist_dict['Top_trx_qtr'].append(int(top_trx_files.strip(".json")))
                        top_trx_data_dist_dict['Top_trx_states'].append(top_trx_states.replace("-"," "))
                        
                    #pincodes    
                    for i in top_json_file_data['data']['pincodes']:
                        top_trx_data_pincode_dict['Top_trx_pincodes'].append(i['entityName'])
                        top_trx_data_pincode_dict['top_trx_pincodes_count'].append(i['metric']['count'])
                        top_trx_pincodes_amt=i['metric']['amount']
                        top_trx_data_pincode_dict['top_trx_pincodes_amount'].append(round(top_trx_pincodes_amt,2))
                        top_trx_data_pincode_dict['Top_trx_years'].append(top_trx_years)
                        top_trx_data_pincode_dict['Top_trx_qtr'].append(int(top_trx_files.strip(".json")))
                        top_trx_data_pincode_dict['Top_trx_states'].append(top_trx_states.replace("-"," "))
                        

    #top Transaction District Dataframe:
    Top_trx_dist_data_df=pd.DataFrame(top_trx_data_dist_dict)
    #print(Top_trx_dist_data_df)
    #7400 rows

    #top Transaction Pincode Dataframe:
    Top_trx_pincode_df=pd.DataFrame(top_trx_data_pincode_dict)
    #print(Top_trx_pincode_df)
    #8924 rows
    return (Top_trx_dist_data_df,Top_trx_pincode_df)

#print(top_trx_data())





 #TOP insurance data :           

def top_ins_data():

    #Top insurance Dictionary: 
    top_ins_data_dist_dict={ 'Top_ins_state':[],'Top_ins_dist':[],'Top_ins_year':[],'Top_ins_qtr':[],'Top_ins_amount':[],'Top_ins_count':[] }

    top_ins_data_pincode_dict={ 'Top_ins_state':[],'Top_ins_pincode':[],'Top_ins_year':[],'Top_ins_qtr':[],'Top_ins_amount':[],'Top_ins_count':[] }


    top_ins_path=os.listdir("/workspaces/Phonepe_Pulse_Data/pulse/data/top/insurance/country/india/state/") 
    #print(top_ins_path)
    for top_ins_states in top_ins_path:
        top_ins_years_path=os.listdir("/workspaces/Phonepe_Pulse_Data/pulse/data/top/insurance/country/india/state"+"/"+top_ins_states+"/")
        #print(top_ins_years_path)

        for top_ins_years in top_ins_years_path:
            top_ins_file_path=os.listdir("/workspaces/Phonepe_Pulse_Data/pulse/data/top/insurance/country/india/state"+"/"+top_ins_states+"/"+top_ins_years+"/")
            #print(top_ins_file_path)
            
            for top_ins_files in top_ins_file_path:
                with open("/workspaces/Phonepe_Pulse_Data/pulse/data/top/insurance/country/india/state"+"/"+top_ins_states+"/"+top_ins_years+"/"+top_ins_files,"r")as top_ins_file:
                    top_ins_file_data=js.load(top_ins_file)
                    #print(top_ins_file_data)

                    #district
                    for i in top_ins_file_data['data']['districts']:
                        top_ins_data_dist_dict['Top_ins_dist'].append(i['entityName'])
                        top_ins_data_dist_dict['Top_ins_count'].append(i['metric']['count'])
                        top_ins_data_dist_dict['Top_ins_amount'].append(round(i['metric']['amount'],2))
                        top_ins_data_dist_dict['Top_ins_state'].append(top_ins_states.replace("-"," "))
                        top_ins_data_dist_dict['Top_ins_qtr'].append(int(top_ins_files.strip(".json")))
                        top_ins_data_dist_dict['Top_ins_year'].append(top_ins_years)


                    #pincodes
                    for i in top_ins_file_data['data']['pincodes']:
                        top_ins_data_pincode_dict['Top_ins_pincode'].append(i['entityName'])
                        top_ins_data_pincode_dict['Top_ins_count'].append(i['metric']['count'])
                        top_ins_data_pincode_dict['Top_ins_amount'].append(round(i['metric']['amount'],2))
                        top_ins_data_pincode_dict['Top_ins_state'].append(top_ins_states.replace("-"," "))
                        top_ins_data_pincode_dict['Top_ins_qtr'].append(int(top_ins_files.strip(".json")))
                        top_ins_data_pincode_dict['Top_ins_year'].append(top_ins_years)


    #Top _insurance_dict

    top_ins_data_dist_df=pd.DataFrame(top_ins_data_dist_dict)
    #print(top_ins_data_dist_df)
    #4711 Rows

    top_ins_data_pincode_df=pd.DataFrame(top_ins_data_pincode_dict)
    #print(top_ins_data_pincode_df)
    #5601 Rows
    return (top_ins_data_dist_df,top_ins_data_pincode_df)


#top_ins_data_pincode_df.to_csv('top_ins_data_pincode.csv',index='False')




#
#top user data:
def top_user_data():


    Top_user_district_dict={ 'top_user_state':[],'top_user_district':[],'top_user_years':[],'top_user_qtr':[],'top_reg_user_count':[] }

    top_user_path=os.listdir("/workspaces/Phonepe_Pulse_Data/pulse/data/top/user/country/india/state/")
    #print(top_user_path)


    for top_user_states in top_user_path:
        top_user_years_path=os.listdir("/workspaces/Phonepe_Pulse_Data/pulse/data/top/user/country/india/state"+"/"+top_user_states+"/")
        #print(top_user_years_path)

        for top_user_years in top_user_years_path:
            top_user_file_path=os.listdir("/workspaces/Phonepe_Pulse_Data/pulse/data/top/user/country/india/state"+"/"+top_user_states+"/"+top_user_years+"/")

            for top_user_files in top_user_file_path:
                with open("/workspaces/Phonepe_Pulse_Data/pulse/data/top/user/country/india/state"+"/"+top_user_states+"/"+top_user_years+"/"+top_user_files,"r")as top_user_file:
                    top_user_file_data=js.load(top_user_file)
                    

                    #districts:    
                    for i in top_user_file_data['data']['districts']:
                        Top_user_district_dict['top_user_district'].append(i['name'])
                        Top_user_district_dict['top_reg_user_count'].append(i['registeredUsers'])
                        Top_user_district_dict['top_user_state'].append(top_user_states.replace("-"," "))
                        Top_user_district_dict['top_user_qtr'].append(int(top_user_files.strip(".json")))
                        Top_user_district_dict['top_user_years'].append(int(top_user_years))



    Top_user_district_dict
    

    Top_user_district_dict_df=pl.DataFrame(Top_user_district_dict)
    
    #output=Top_user_district_dict_df.group_by('top_user_district').agg
    #('top_reg_user_count','top_user_years')
    #(pl.col("top_user_years")== 2020)
    
    return Top_user_district_dict_df

print(top_user_data())
#7400 rows







