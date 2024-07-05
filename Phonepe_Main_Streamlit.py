import streamlit as st

st.base="dark"
st.primaryColor="purple"

st.image("https://upload.wikimedia.org/wikipedia/commons/7/71/PhonePe_Logo.svg",clamp='True',width=500)
st.title("PHONEPE PULSE DATA ANALYSIS")
st.header("3 Types of Data Analysis")
st.button(":question:&:pencil: Aggregated Data Analysis")
    #st.switch_page("pages/Youtube_Data_Harvesting_Warehousing.py")
st.button(":question:&:pencil: Map Data Analysis")
    #st.write("***Analyze Based on the Given Question***")
    #st.switch_page("pages/Youtube_Channel_Query_Analysis.py")
st.button(":question:&:pencil: Top Data Analysis")
 
