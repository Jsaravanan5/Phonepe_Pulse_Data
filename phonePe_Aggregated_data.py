import os
import json as js
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns


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





