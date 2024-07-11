#https://blog.devart.com/mysql-command-line-client.html#:~:text=MySQL%20client%20is%20a%20common,databases%20stored%20on%20the%20server.

import sqlite3
import pandas as pd
import os
import json as js

from sqlalchemy import create_engine

con = sqlite3.connect("test.db")
cur = con.cursor()

cur.execute("CREATE TABLE IF NOT EXISTS AGG(Transaction_Type VARCHAR(255),Transaction_States VARCHAR(255),Transaction_Years INT,Transaction_Quarter INT,Transaction_Count INT,Transaction_Amount INT)")

#cur.execute("ALTER TABLE AGG DROP COLUMN index;")


con.commit()

def agg_trx_func():

    # Insurance_dictionary to capture the information:
    agg_trx_data_dict={ "Trx_States":[],"Trx_Years":[],"Trx_Qtr":[],"Agg_txn_type":[],"Agg_txn_count":[],"Agg_txn_amount":[] 
                    } 

    ins_states_path=os.listdir("/workspaces/Phonepe_Pulse_Data/pulse/data/aggregated/transaction/country/india/state/")
    agg_ins_path=list(ins_states_path)
    #print(agg_ins_path)

    for trx_states in agg_ins_path:
        agg_ins_years_path=os.listdir("/workspaces/Phonepe_Pulse_Data/pulse/data/aggregated/transaction/country/india/state/"+trx_states+"/")
        #print(ins_years_path)

        for  trx_years in agg_ins_years_path:
            agg_ins_file_path=os.listdir("/workspaces/Phonepe_Pulse_Data/pulse/data/aggregated/transaction/country/india/state/"+trx_states+"/"+trx_years+"/")
            #print(agg_ins_file_path)

            for file in agg_ins_file_path:
                with open("/workspaces/Phonepe_Pulse_Data/pulse/data/aggregated/transaction/country/india/state/"+trx_states+"/"+trx_years+"/"+file,"r") as json_file :
                    agg_json_txn_file=js.load(json_file)
                    
                
                for i in agg_json_txn_file['data']['transactionData']:
                    agg_txn_type=i['name']
                    agg_txn_count=i['paymentInstruments'][0]['count']
                    agg_txn_amount=i['paymentInstruments'][0]['amount']
                    agg_trx_data_dict['Trx_States'].append(trx_states.replace("-"," "))
                    agg_trx_data_dict['Trx_Years'].append(int(trx_years))
                    agg_trx_data_dict['Trx_Qtr'].append(int(file.strip(".json")))
                    agg_trx_data_dict['Agg_txn_type'].append(agg_txn_type)
                    agg_trx_data_dict['Agg_txn_count'].append(agg_txn_count)
                    agg_trx_data_dict['Agg_txn_amount'].append(int(agg_txn_amount))
                    
                    
                    Agg_trx_df=pd.DataFrame(agg_trx_data_dict)


    #agg_trx_df = Agg_trx_df
    return  Agg_trx_df

print(agg_trx_func())

engine=create_engine('sqlite:///test.db', echo=False,pool_size=10, max_overflow=20)

agg_trx_func().to_sql('AGG', con=engine, if_exists='append',chunksize=2)








cur.execute("SELECT * FROM AGG")

