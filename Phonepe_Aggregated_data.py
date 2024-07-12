import os
import json as js
import pandas as pd
import matplotlib.pyplot as plt
import polars as pl
import mysql.connector
import streamlit as st
import plotly.express as px 

#Function to get Aggregated_Transaction:

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
                    
                    
                    Agg_trx_df=pl.DataFrame(agg_trx_data_dict)


    #agg_trx_df = Agg_trx_df
    #fig = px.bar(agg_trx_df, x="Agg_txn_type", y="Agg_txn_count", title="Aggregated_Transaction")
    #fig=px.sunburst(agg_trx_df, path=['Agg_txn_type', 'Trx_States','Trx_Years','Trx_Qtr'], values='Agg_txn_count')
    #fig = px.line(agg_trx_df, x="Trx_Years", y="Agg_txn_count", title="Aggregated_Transaction")
    #fig = px.pie(agg_trx_df, names="Trx_States", values="Agg_txn_count", title="Aggregated_Transaction")
    # values='pop', names='country'
    #fig.show() 
    #st.bar_chart(data=agg_trx_df,x='Agg_txn_type',y='Agg_txn_count', color="#f0f", width=400, height=400, use_container_width=True)
                                   
    return Agg_trx_df

#4496
#print(agg_trx_func())
df=agg_trx_func()



fig = px.bar(df, x="Trx_Years", y="Agg_txn_count",color='Trx_Qtr',title="Aggregated_Transaction year-Wise")
st.plotly_chart(fig,use_container_width=True, key='map_trx_state', on_select="rerun", selection_mode=('points'))

fig1=px.sunburst(df, path=['Trx_States',"Trx_Years",'Agg_txn_type'], values='Agg_txn_amount')
st.plotly_chart(fig1,use_container_width=True, theme="streamlit", key='map_trx_state', on_select="rerun", selection_mode=('points'))

fig2 = px.pie(df, names="Trx_States", values="Agg_txn_count", title="Aggregated_Transaction State Wise")
st.plotly_chart(fig2,use_container_width=True, theme="streamlit", key='map_trx_state', on_select="rerun", selection_mode=('points'))

fig4= px.line(df, x="Trx_States", y="Agg_txn_count", title="Aggregated_Transaction",color='Agg_txn_type')
st.plotly_chart(fig4,use_container_width=True, theme="streamlit", key='map_trx_state', on_select="rerun", selection_mode=('points'))



# Function to get Aggregated_insurance:
def agg_ins_func():
    
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
                    Insurance_agg_data['Ins_Years'].append(int(ins_years))
                    Insurance_agg_data['Ins_Qtr'].append(int(file.strip(".json")))
                    Insurance_agg_data['Ins_txn_type'].append(agg_ins_txn_type)
                    Insurance_agg_data['Ins_txn_count'].append(agg_ins_txn_count)
                    Insurance_agg_data['Ins_txn_amount'].append(int(agg_ins_txn_amount))

    
                    Agg_Insurance_df=pl.DataFrame(Insurance_agg_data)

                                    
    return Agg_Insurance_df

#insurance Dataframe   
#574
#print(agg_ins_func())


def agg_user_func():
    #Agg_User_Data_Dictionary:
    user_agg_data= {
                     "User_States":[],
                     "User_Years":[],
                     "User_Qtr":[],
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
                        user_agg_data['User_States'].append(user_states.replace("-"," "))
                        user_agg_data["User_Years"].append(int(user_years))
                        user_agg_data["User_Qtr"].append(int(file.strip(".json")))
                        


    #User DataFrame                
    Agg_Users_df=pl.DataFrame(user_agg_data) 

 

    return Agg_Users_df               

#print(agg_user_func())
#6732

#question:
#1. agg_txn
#  1.1 which state has more
#  
#1. agg_ins
#1. agg_ins
#

#creating a phonepe Database:

#mydb = mysql.connector.connect(
# host="localhost",
# user="sqluser",
 #password="password"
#)
#mycursor = mydb.cursor()
#mycursor.execute("CREATE DATABASE phonepedatabase")

#MySQL connection configuration
mysql_host = "localhost"
mysql_user = "sqluser"
mysql_password = "password"
mysql_database = "phonepedatabase"
mysql_port = "3306"  


def connect_to_mysql():
    try:
        db_connect = mysql.connector.connect(
           host=mysql_host,
          user=mysql_user,
           password=mysql_password,
           database=mysql_database,
          port=mysql_port
        )
        print("Connected to MySQL database successfully")
        return db_connect

    except mysql.connector.Error as e:
       print("Error connecting to MySQL database:", e)
       return None


def create_tables(db_connect):
    #Creating a cursor object using the cursor() method
    
    cursor=db_connect.cursor()
     # Table creation queries
    AGG_TRX_table_query = """
    CREATE TABLE IF NOT EXISTS agg_trx_data (
        Tranasction_Type_Id INT AUTO_INCREMENT,
        Transaction_Type VARCHAR(255),
        Transaction_States VARCHAR(255),
        Transaction_Years INT,
        Transaction_Quarter INT,
        Transaction_Count INT,
        Transaction_Amount INT,
        PRIMARY KEY(Agg_txn_type_Id)
    
    )
    """
    AGG_INS_table_query = """
    CREATE TABLE IF NOT EXISTS agg_ins_data (
        Tranasction_Type_Id INT NOT NULL AUTO_INCREMENT,
        Transaction_Type VARCHAR(255),
        Insurance_States VARCHAR(255),
        Insurance_Years INT,
        Insurance_Quarter INT,
        Insurance_Count INT,
        Insurance_Amount INT,        
        PRIMARY KEY(Tranasction_Type_Id)
    )
    """

    AGG_USER_table_query = """
    CREATE TABLE IF NOT EXISTS agg_user_data (
        User_Device_Brand_Id INT NOT NULL AUTO_INCREMENT,
        User_Device_Brand VARCHAR(255),
        User_States VARCHAR(255),
        User_Years INT,
        User_Quarter INT,
        User_Device_Count INT,
        User_Device_Percent FLOAT,
        PRIMARY KEY(User_Device_Brand_Id)
    )
    """

    try:
        # Execute table creation queries
        cursor.execute(AGG_TRX_table_query)
        cursor.execute(AGG_INS_table_query)
        cursor.execute(AGG_USER_table_query)

        db_connect.commit()
        print("Tables created successfully in MySQL")
    except mysql.connector.Error as e:
        print("Error creating tables in MySQL:", e)
        db_connect.rollback()
    finally:
        cursor.close()


#Table Data_Insert:
#insert Panda Data frame into MYSQL as  
#link as follows: https://saturncloud.io/blog/writing-a-pandas-dataframe-to-mysql/
#https://www.dataquest.io/blog/sql-insert-tutorial/

# Code Addition:
#Import Necessary Library: 
#from sqlalchemy import create_engine


# Step 1: Create a DataFrame with the data
#data = {'name': ['Alice', 'Bob'],
#        'address': ['Street 123', 'Avenue 456']}
#df = pd.DataFrame(data)

#function that return the Pandas Dataframe:
# agg_trx_func() 

# Step 2: Create a SQLAlchemy engine to connect to the MySQL database
# engine = create_engine("mysql+mysqlconnector://root:new_password@localhost/mydatabase")

#engine=create_engine("mysql+mysqlconnector://sqluser:password@localhost/phonepedatabase")

# Step 3: Convert the Pandas DataFrame to a format for MySQL table insertion
# df.to_sql('table name', con=engine, if_exists='append', index=False)

#agg_trx_func().to_sql('agg_trx_data', con=engine, if_exists='append', index=False)

#print(agg_trx_func())
#-----------------------------------------------------------------------
#Step 4: Insert the data into the MySQL table (Not Required)
#mycursor = mydb.cursor()

#sql = "INSERT INTO customers (name, address) VALUES (%s, %s)"
#val = ("John", "Highway 21")

#mycursor.execute(sql, val)

#mydb.commit()
#print(mycursor.rowcount, "record inserted.")

#-------------------------------------------------------------------------
def insert_agg_trx_data_to_mysql(db_connect,agg_trx_func):
    cursor = db_connect.cursor()
    try:
        for i in agg_trx_func:
            
            insert_query = """
#            INSERT INTO  (Transaction_Type,Transaction_States,Transaction_Years,Transaction_Quarter,Transaction_Count,Transaction_Amount) 
#           VALUES (%s, %s, %s, %s, %s, %s)
#           """
            # Execute the query with data from the aggregated transaction_data
            cursor.execute(insert_query, (i["Transaction_Type"], i["Transaction_States"], i["Transaction_Years"], i["Transaction_Quarter"], i["Transaction_Count"], i["Transaction_Amount"]))
        
        db_connect.commit()
        return "Success"
    except mysql.connector.Error as e:
        return "Duplicate"
        db_connect.rollback()
    finally:
        cursor.close()



def insert_agg_ins_func_to_mysql(db_connect, agg_ins_func):
    cursor = db_connect.cursor()
    try:
        for i in agg_ins_func:
            
            insert_query = """
            INSERT INTO agg_ins_data (Transaction_Type,Insurance_States,Insurance_Years,Insurance_Quarter,Insurance_Count,Insurance_Amount) 
            VALUES (%s, %s, %s, %s, %s, %s)
            """
            # Execute the query with data from the channel_info list
            cursor.execute(insert_query, (i["Transaction_Type"], i["Insurance_States"], i["Insurance_Years"], i["Insurance_Quarter"], i["Insurance_Count"], i["Insurance_Amount"]))
        
        db_connect.commit()
        return "Success"
    except mysql.connector.Error as e:
        return "Duplicate"
        db_connect.rollback()
    finally:
        cursor.close()




def insert_agg_user_func_to_mysql(db_connect, agg_user_func):
    cursor = db_connect.cursor()
    try:
        for i in agg_user_func:
            
            insert_query = """
            INSERT INTO agg_ins_data (User_Device_Brand,User_States,User_Years,User_Quarter,User_Device_Count,User_Device_Amount) 
            VALUES (%s, %s, %s, %s, %s, %s)
            """
            # Execute the query with data from the channel_info list
            cursor.execute(insert_query, (i["User_Device_Brand"], i["User_States"], i["User_Years"], i["User_Quarter"], i["User_Device_Count"], i["User_Device_Amount"]))
        
        db_connect.commit()
        return "Success"
    except mysql.connector.Error as e:
        return "Duplicate"
        db_connect.rollback()
    finally:
        cursor.close()


st.title(":blue[PHONEPE AGRREGATED INFORMATION]")
st.write("This page will provide you the aggregated phonepe pulse data(transaction,insurance,user) and its insights")


#channel_id_input_placeholder = 'channel_id_input'
#channel_id = st.text_input('Enter your ', key=channel_id_input_placeholder)
#if len(channel_id)>0:
#if st.button("Import Youtube Channel Details"):


 # Establishing connection to MySQL database
#db_connect = connect_to_mysql()
# Creating tables if they don't exist
#if db_connect is not None:
#    create_tables(db_connect)
# Inserting data into MySQL tables
#    insert_agg_trx_data_to_mysql(db_connect,agg_trx_func)
#    insert_agg_ins_func_to_mysql(db_connect, agg_ins_func)
#    insert_agg_user_func_to_mysql(db_connect, agg_user_func)
#else:
 #   st.write("Aggregate information not inserted into tables Succesfully ")
 #   # Closing the MySQL connection
 #   db_connect.close()



















                








