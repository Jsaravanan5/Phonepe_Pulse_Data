#import Library:
import os
import json as js
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import polars as pl


# Top_data >> Transaction:


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
                for i in top_json_file_data['data']['districts']:
                    top_trx_data_dist_dict['Top_trx_dist'].append(i['entityName'])
                    top_trx_data_dist_dict['top_trx_dist_count'].append(i['metric']['count'])
                    top_trx_dist_amt=i['metric']['amount']
                    top_trx_data_dist_dict['top_trx_dist_amount'].append(round(top_trx_dist_amt,2))  #round-off
                    top_trx_data_dist_dict['Top_trx_years'].append(top_trx_years)
                    top_trx_data_dist_dict['Top_trx_qtr'].append(int(top_trx_files.strip(".json")))
                    top_trx_data_dist_dict['Top_trx_states'].append(top_trx_states)
                    
                    
                for i in top_json_file_data['data']['pincodes']:
                    top_trx_data_pincode_dict['Top_trx_pincodes'].append(i['entityName'])
                    top_trx_data_pincode_dict['top_trx_pincodes_count'].append(i['metric']['count'])
                    top_trx_pincodes_amt=i['metric']['amount']
                    top_trx_data_pincode_dict['top_trx_pincodes_amount'].append(round(top_trx_pincodes_amt,2))
                    top_trx_data_pincode_dict['Top_trx_years'].append(top_trx_years)
                    top_trx_data_pincode_dict['Top_trx_qtr'].append(int(top_trx_files.strip(".json")))
                    top_trx_data_pincode_dict['Top_trx_states'].append(top_trx_states)
                       

#top Transaction District Dataframe:
Top_trx_dist_data_df=pd.DataFrame(top_trx_data_dist_dict)
#print(Top_trx_dist_data_df)
#7400 rows

#top Transaction Pincode Dataframe:
Top_trx_pincode_df=pd.DataFrame(top_trx_data_pincode_dict)
#print(Top_trx_pincode_df)
#8924 rows



 #TOP insurance data :           

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

                for i in top_ins_file_data['data']['districts']:
                    top_ins_data_dist_dict['Top_ins_dist'].append(i['entityName'])
                    top_ins_data_dist_dict['Top_ins_count'].append(i['metric']['count'])
                    top_ins_data_dist_dict['Top_ins_amount'].append(round(i['metric']['amount'],2))
                    top_ins_data_dist_dict['Top_ins_state'].append(top_ins_states)
                    top_ins_data_dist_dict['Top_ins_qtr'].append(int(top_ins_files.strip(".json")))
                    top_ins_data_dist_dict['Top_ins_year'].append(top_ins_years)



                for i in top_ins_file_data['data']['pincodes']:
                    top_ins_data_pincode_dict['Top_ins_pincode'].append(i['entityName'])
                    top_ins_data_pincode_dict['Top_ins_count'].append(i['metric']['count'])
                    top_ins_data_pincode_dict['Top_ins_amount'].append(round(i['metric']['amount'],2))
                    top_ins_data_pincode_dict['Top_ins_state'].append(top_ins_states)
                    top_ins_data_pincode_dict['Top_ins_qtr'].append(int(top_ins_files.strip(".json")))
                    top_ins_data_pincode_dict['Top_ins_year'].append(top_ins_years)


#Top _insurance_dict

top_ins_data_dist_df=pd.DataFrame(top_ins_data_dist_dict)
#print(top_ins_data_dist_df)
#4711 Rows

top_ins_data_pincode_df=pd.DataFrame(top_ins_data_pincode_dict)
#print(top_ins_data_pincode_df)
#5601 Rows


#top_ins_data_pincode_df['Top_ins_pincode']=top_ins_data_pincode_df['Top_ins_pincode']=='600050' 
#top_ins_data_pincode_df['Top_ins_state']=top_ins_data_pincode_df['Top_ins_state']=='tamil-nadu'
#print(top_ins_data_pincode_df)

#top_ins_data_pincode_df.to_csv('top_ins_data_pincode.csv',index='False')




#
#top user data:

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
                    Top_user_district_dict['top_user_state'].append(top_user_states)
                    Top_user_district_dict['top_user_qtr'].append(int(top_user_files.strip(".json")))
                    Top_user_district_dict['top_user_years'].append(top_user_years)



Top_user_district_dict

Top_user_district_dict_df=pl.DataFrame(Top_user_district_dict)


#print(Top_user_district_dict_df)












                   



