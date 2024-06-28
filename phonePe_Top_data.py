#import Library:
import os
import json as js
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns


# Top_data >> Transaction:


top_trx_path=os.listdir("/workspaces/Phonepe_Pulse_Data/pulse/data/top/transaction/country/india/state/")
#print(top_trx_path)



for top_trx_states in top_trx_path:
    top_trx_years_path=os.listdir("/workspaces/Phonepe_Pulse_Data/pulse/data/top/transaction/country/india/state"+"/"+top_trx_states+"/")
    #print(top_trx_years_path)

    for top_trx_years in top_trx_years_path:
        top_trx_file_path=os.listdir("/workspaces/Phonepe_Pulse_Data/pulse/data/top/transaction/country/india/state"+"/"+top_trx_states+"/"+top_trx_years+"/")
        #print(top_trx_file_path)
        