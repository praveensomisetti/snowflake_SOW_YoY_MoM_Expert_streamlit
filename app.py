import streamlit as st
import snowflake.connector
import pandas as pd
from dotenv import load_dotenv
import os
# Load environment variables from .env file
load_dotenv()

# Function to get Snowflake credentials from environment variables
def get_snowflake_credentials():
    return {
        "user": os.getenv("USER"),
        "password": os.getenv("PASSWORD"),
        "account": os.getenv("ACCOUNT")
    }

# Function to read query from a file
def read_query(file_path):
    with open(file_path, 'r') as file:
        query = file.read()
    return query

# Function to get data from Snowflake using a query
def get_data_from_query(credentials, query):
    conn = snowflake.connector.connect(
        user=credentials['user'],
        password=credentials['password'],
        account=credentials['account']
    )
    cur = conn.cursor()
    cur.execute(query)
    data = cur.fetchall()
    cur.close()
    conn.close()
    return data

# Streamlit app
# def main():
st.title("SOW Won Expert Trends by Year,Month and Week")

# Load credentials
credentials = get_snowflake_credentials()

# Read queries from files
query_view1 = read_query('year.txt')
query_view2 = read_query('month.txt')
query_view3 = read_query('week.txt')

# Create tabs for each view
view1_tab, view2_tab, view3_tab = st.tabs(["year over year", "month on month", "week over week"])

with view1_tab:
    st.header("Data from year over year")
    data_view1 = get_data_from_query(credentials, query_view1)
    df_view1 = pd.DataFrame(data_view1, columns=['APPROVED_YEAR', 'DISTINCT_FREELANCER_COUNT', 'DISTINCT_FREELANCER_SOW_SIGNED_COUNT','SOW_SIGNED_WITHIN_30_DAYS_COUNT','AVG_DAYS_TO_FIRST_SOW_SIGNED'])  # Adjust column names
    df_view1['APPROVED_YEAR'] = df_view1['APPROVED_YEAR'].astype(str)
    st.dataframe(df_view1, hide_index=True)
    #st.line_chart(df_view1.set_index('APPROVED_YEAR'))  # Adjust the column used for the x-axis

with view2_tab:
    st.header("Data from month on month")
    data_view2 = get_data_from_query(credentials, query_view2)
    df_view2 = pd.DataFrame(data_view2, columns=['APPROVED_YEAR','APPROVED_MONTH','DISTINCT_FREELANCER_COUNT', 'DISTINCT_FREELANCER_SOW_SIGNED_COUNT','SOW_SIGNED_WITHIN_30_DAYS_COUNT','AVG_DAYS_TO_FIRST_SOW_SIGNED'])  # Adjust column names
    df_view2['APPROVED_YEAR'] = df_view2['APPROVED_YEAR'].astype(str)
    st.dataframe(df_view2, hide_index=True)
    # st.line_chart(df_view2.set_index('Column1'))  # Adjust the column used for the x-axis

with view3_tab:
    st.header("Data from week over week")
    data_view3 = get_data_from_query(credentials, query_view3)
    df_view3 = pd.DataFrame(data_view3, columns=['APPROVED_YEAR','APPROVED_MONTH','APPROVED_WEEK','DISTINCT_FREELANCER_COUNT', 'DISTINCT_FREELANCER_SOW_SIGNED_COUNT','SOW_SIGNED_WITHIN_30_DAYS_COUNT','AVG_DAYS_TO_FIRST_SOW_SIGNED'])  # Adjust column names
    df_view3['APPROVED_YEAR'] = df_view3['APPROVED_YEAR'].astype(str)
    st.dataframe(df_view3, hide_index=True)
    # st.line_chart(df_view3.set_index('Column1'))  # Adjust the column used for the x-axis

# if __name__ == "__main__":
#     main()
