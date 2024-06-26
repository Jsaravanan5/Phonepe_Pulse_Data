import os
import json as js
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns


# Insurance_dictionary to capture the information:
Insurance_agg_data={
                     "Ins_States":[],
                     "Ins_Years":[],
                     "Ins_Qtr":[],
                     "Ins_txn_type":[],
                     "Ins_txn_count":[],
                     "Ins_txn_amount":[] 
                    } 

#insurance
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
                #print(json_file)
            
            for data in agg_json_ins_file['data']['transactionData']:
                agg_ins_txn_type=data['name']
                agg_ins_txn_count=data['paymentInstruments'][0]['count']
                agg_ins_txn_amount=data['paymentInstruments'][0]['amount']
                Insurance_agg_data['Ins_States'].append(ins_states)
                Insurance_agg_data['Ins_Years'].append(ins_years)
                Insurance_agg_data['Ins_Qtr'].append(int(file.strip(".json")))
                Insurance_agg_data['Ins_txn_type'].append(agg_ins_txn_type)
                Insurance_agg_data['Ins_txn_count'].append(agg_ins_txn_count)
                Insurance_agg_data['Ins_txn_amount'].append(agg_ins_txn_amount)

#insurance Dataframe            
Agg_Insurance_df=pd.DataFrame(Insurance_agg_data)

#print(Agg_Insurance_df)




#User_Data_Dictionary:

user_agg_data={
                     "User_States":[],
                     "User_Years":[],
                     "User_Qtr":[],
                     "User_Device_Brand":[],
                     "User_Device_count":[],
                     "User_Device_percent":[]
                    } 



#aggregated_User_data
user_states_path=os.listdir("/workspaces/Phonepe_Pulse_Data/pulse/data/aggregated/user/country/india/state/")
agg_user_path=list(user_states_path)

#print(agg_user_path)

for user_states in agg_user_path:
    agg_user_years_path=os.listdir("/workspaces/Phonepe_Pulse_Data/pulse/data/aggregated/user/country/india/state/"+user_states+"/")
    #print(agg_user_years_path)

    for user_years in agg_ins_years_path:
        agg_user_file_path=os.listdir("/workspaces/Phonepe_Pulse_Data/pulse/data/aggregated/user/country/india/state/"+user_states+"/"+user_years+"/")
        #print(agg_user_file_path)

        for file in agg_user_file_path:
            with open("/workspaces/Phonepe_Pulse_Data/pulse/data/aggregated/user/country/india/state/"+user_states+"/"+user_years+"/"+file,"r") as json_file :
                agg_json_user_file=js.load(json_file)
                #print(agg_json_user_file)
            
            data=agg_json_user_file['data']['usersByDevice']
            if data is not None:
                for i in data: 
                   agg_user_device_brand=i['brand']
                   agg_user_device_count=i['count']
                   agg_user_device_percent=i['percentage']
                   user_agg_data["User_Device_Brand"].append(agg_user_device_brand)
                   user_agg_data["User_Device_count"].append(agg_user_device_count)
                   user_agg_data["User_Device_percent"].append(agg_user_device_percent)
                   user_agg_data['User_States'].append(user_states)
                   user_agg_data["User_Years"].append(user_years)
                   user_agg_data["User_Qtr"].append(int(file.strip(".json")))


#User DataFrame                
Agg_Users_df=pd.DataFrame(user_agg_data)                
#print(Agg_Users_df)
            
                








