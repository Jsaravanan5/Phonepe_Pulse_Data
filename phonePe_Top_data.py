#import Library:
import os
import json as js
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns


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
print(Top_trx_dist_data_df)
#7400 rows

#top Transaction Pincode Dataframe:
Top_trx_pincode_df=pd.DataFrame(top_trx_data_pincode_dict)
#print(Top_trx_pincode_df)
#8924 rows

             

