import streamlit as st
from models.table import Table  
from models.view import View



st.title("Snowflake ETL Builder") 

# Sidebar Navigation
st.sidebar.title("Navigation")
layers = ["Home", "Bronze", "Silver", "Gold"]
selected_layer = st.sidebar.radio("Select Layer", layers)

# Main Page Logic
if selected_layer == "Home":
    st.write("### Welcome to the ETL Builder")
    st.info("Select a layer from the sidebar to begin.")

elif selected_layer == "Bronze":
    st.header("Bronze Layer - Raw Data")
    

    st.write("#### Preview of a Standard Bronze Table:")
    
    # Instantiate a fake table
    bronze_table = Table(
        schema="BRONZE_DB.RAW", 
        name="LANDING_USERS", 
        columns="id INT, json_data VARIANT, load_date TIMESTAMP"
    )
    
    # Display the DDL using Streamlit's code block
    st.code(bronze_table.create_ddl(), language='sql')

elif selected_layer == "Silver":
    st.header("Silver Layer - Cleaned Data")
    st.write("This section is under construction.")

elif selected_layer == "Gold":
    st.header("Gold Layer - Aggregated Facts")
    st.write("This section is under construction.")