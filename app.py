import streamlit as st
import snowflake.connector
import pandas as pd
import numpy as np

# Initialize connection.
# Uses st.cache_resource to only run once.
@st.cache_resource
def init_connection():
    return snowflake.connector.connect(
        **st.secrets["snowflake"], client_session_keep_alive=True
    )

conn = init_connection()

# Uses st.cache_data to only rerun when the query changes or after 10 min.
# @st.cache_data(ttl=600)
def run_query(query):
    with conn.cursor() as cur:
        cur.execute(query)
        dat = cur.fetchall()
        df = pd.DataFrame(dat, columns=[col[0] for col in cur.description])
        return df

st.header("Snowflake Table:")

df = run_query(f"select * from FOOD_INSPECTIONS_SMALL")

st.dataframe(df)
