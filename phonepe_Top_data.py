#import Library:
import os
import json as js
import pandas as pd
import matplotlib.pyplot as plt
import polars as pl
import mysql.connector
import streamlit as st
import plotly.express as px


# Top_data >> Transaction:

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
                        top_trx_data_dist_dict['Top_trx_years'].append(int(top_trx_years))
                        top_trx_data_dist_dict['Top_trx_qtr'].append(int(top_trx_files.strip(".json")))
                        top_trx_data_dist_dict['Top_trx_states'].append(top_trx_states.replace("-"," "))
                        
                    #pincodes    
                    for i in top_json_file_data['data']['pincodes']:
                        top_trx_data_pincode_dict['Top_trx_pincodes'].append(i['entityName'])
                        top_trx_data_pincode_dict['top_trx_pincodes_count'].append(i['metric']['count'])
                        top_trx_pincodes_amt=i['metric']['amount']
                        top_trx_data_pincode_dict['top_trx_pincodes_amount'].append(round(top_trx_pincodes_amt,2))
                        top_trx_data_pincode_dict['Top_trx_years'].append(int(top_trx_years))
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
    return (Top_trx_dist_data_df)

#print(top_trx_data())



df = top_trx_data()
print(df.describe())

#fig = px.bar(df, x='Top_trx_states', y="top_trx_dist_amount",color='Top_trx_dist')
#st.plotly_chart(fig,use_container_width=True, key='Top_trx_states', on_select="rerun", selection_mode=('points'))

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
                        top_ins_data_dist_dict['Top_ins_year'].append(int(top_ins_years))


                    #pincodes
                    for i in top_ins_file_data['data']['pincodes']:
                        top_ins_data_pincode_dict['Top_ins_pincode'].append(i['entityName'])
                        top_ins_data_pincode_dict['Top_ins_count'].append(i['metric']['count'])
                        top_ins_data_pincode_dict['Top_ins_amount'].append(round(i['metric']['amount'],2))
                        top_ins_data_pincode_dict['Top_ins_state'].append(top_ins_states.replace("-"," "))
                        top_ins_data_pincode_dict['Top_ins_qtr'].append(int(top_ins_files.strip(".json")))
                        top_ins_data_pincode_dict['Top_ins_year'].append(int(top_ins_years))


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
    

    Top_user_district_dict_df=pd.DataFrame(Top_user_district_dict)
    
    #output=Top_user_district_dict_df.group_by('top_user_district').agg
    #('top_reg_user_count','top_user_years')
    #(pl.col("top_user_years")== 2020)
    
    return Top_user_district_dict_df

#print(top_user_data())
#7400 rows

df2=top_user_data()

#fig6 = px.pie(df2, names="top_user_years", values="top_reg_user_count", title="No.Registration.years")
#st.plotly_chart(fig6,use_container_width=True, theme="streamlit", key='map_trx_state', on_select="rerun", selection_mode=('points'))


def execute_query(query):
    con = mysql.connector.connect(
        host="localhost",
        user="sqluser",
        password="password",
        database="phonepe",
        port="3306"
    )
    cursor = con.cursor()
    cursor.execute(query)
    data = cursor.fetchall()
    con.close()
    return data


st.header("PHONEPE TOP 10 STATES BASED ON TRANSACTION, INSURANCE & USER")


#subplots Reference:
from plotly.subplots import make_subplots
import plotly.graph_objects as go


# Create a 1 row, 2 column subplots figure
fig = make_subplots(rows=1, cols=2)


df['top_trx_dist_amount']=df['top_trx_dist_amount'].sort_values(ascending=False)
# Create the first bar chart for `top_trx_states` vs. `top_trx_dist_amount`
bar1 = go.Bar(x=df['Top_trx_dist'], y=df['top_trx_dist_amount'])#legend='District Wise Transaction Amount')


# Add the first bar chart directly to the figure using `add_trace`
fig.add_trace(bar1, row=1, col=1)

# Update layout options for the first subplot (optional)
fig.update_xaxes(title_text='State', row=1, col=1)  # Set x-axis title
fig.update_yaxes(title_text='Total Transaction Amount', row=1, col=1)  # Set y-axis title

# Create the second bar chart for `top_user_years` vs. `top_reg_user_count`
bar2 = go.Bar(x=df2["top_user_years"], y=df2["top_reg_user_count"])

# Add the second bar chart directly to the figure using `add_trace`
fig.add_trace(bar2, row=1, col=2)

# Update layout options for the second subplot (optional)
fig.update_xaxes(title_text='Year', row=1, col=2)  # Set x-axis title
fig.update_yaxes(title_text='Number of Registered Users', row=1, col=2)  # Set y-axis title

# Update the overall figure layout
fig.update_layout(height=600, width=800, title_text="Top district Subplots")

# Render the figure using Streamlit
#st.plotly_chart(fig,theme='streamlit')

#https://plotly.com/python/sliders/

fig2 = px.bar(df, x='Top_trx_dist', y="top_trx_dist_amount",color='Top_trx_states',animation_frame='Top_trx_years',animation_group='Top_trx_states',range_y=[0,1753799000000]) #range_y=[0,1753799000000] 
st.plotly_chart(fig2,theme='streamlit')

















                   



