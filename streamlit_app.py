import streamlit as st
import pandas as pd
from utils.snowflake_connector import get_session
from utils.data_provider import get_data_provider
from models.table import Table  
from models.view import View
from models.dynamic_table import DynamicTable



st.title("Snowflake ETL Builder") 

# Sidebar Navigation
st.sidebar.title("Navigation")
layers = ["Home", "Bronze", "Silver", "Gold"]
selected_layer = st.sidebar.radio("Select Layer", layers)


# Main Page Logic
if selected_layer == "Home":
    st.write("### Welcome to the ETL Builder")
    
    # Connection Check
    #session = get_session()
    #if session:
    #    st.success(f"Connected to Snowflake! Current Role: {session.get_current_role()}")
    #    st.info(f"Current Warehouse: {session.get_current_warehouse()}")
    #else:
    #    st.warning("No active Snowflake session found.")


elif selected_layer == "Bronze":
    st.header("Bronze Layer - Raw Data")
    

    st.write("#### Preview of a Standard Bronze Table:")
    
    # Instantiate a fake table
    bronze_table = Table(
        schema="DB.BRONZE", 
        name="LANDING_USERS", 
        columns="id INT, json_data VARIANT, load_date TIMESTAMP"
    )
    
    # Display the DDL using Streamlit's code block
    st.code(bronze_table.create_ddl(), language='sql')


elif selected_layer == "Silver":
    st.header("Silver Layer - Builder")
    provider = get_data_provider()

    #1: Source Selection 
    st.subheader("1. Select Source")
    source_tables = provider.get_tables("BRONZE_DB.RAW") 
    selected_source = st.selectbox("Choose a Source Table", source_tables)

    #2: Column Mapping
    st.subheader("2. Define Transformations")
    
    # Get raw columns: [("ID", "NUMBER"), ("NAME", "VARCHAR")...]
    raw_columns = provider.get_columns(selected_source)
    
    # Convert to DataFrame for the Editor
    # add empty columns for 'Rename To' and 'Transformation Logic'
    df_cols = pd.DataFrame(raw_columns, columns=["Source Column", "Data Type"])
    df_cols["Target Column Name"] = df_cols["Source Column"] # Default to same name
    df_cols["Transformation"] = "" # Empty by default
    df_cols["Include"] = True # Checkbox to keep/drop column

    # DISPLAY THE EDITOR
    st.write("Edit the columns below. Uncheck 'Include' to drop a column.")
    edited_df = st.data_editor(
        df_cols,
        column_config={
            "Include": st.column_config.CheckboxColumn("Keep?", help="Select to include in target"),
            "Source Column": st.column_config.TextColumn("Source", disabled=True), # Read-only
            "Data Type": st.column_config.TextColumn("Type", disabled=True),       # Read-only
            "Target Column Name": st.column_config.TextColumn("Target Name"),
            "Transformation": st.column_config.TextColumn("SQL Logic (Optional)", help="e.g. CAST(x AS INT) or UPPER(x)"),
        },
        hide_index=True,
        use_container_width=True
    )

    # 3: SQL Generation Logic ---
    st.divider()
    st.subheader("3. Preview SQL")
    
    if st.button("Preview Projection SQL"):
        #need to loop through the edited rows and build the SELECT list
        select_parts = []
        
        for index, row in edited_df.iterrows():
            if row["Include"]:
                src = row["Source Column"]
                tgt = row["Target Column Name"]
                logic = row["Transformation"]
                
                # Logic: If there is a transformation, use it. Otherwise use source column.
                
                if logic:
                    col_sql = f"{logic} AS {tgt}"
                elif src != tgt:
                    col_sql = f"{src} AS {tgt}"
                else:
                    col_sql = src 
                
                select_parts.append(col_sql)
        
        # Join them with commas
        final_select = "SELECT \n    " + ",\n    ".join(select_parts) + f"\nFROM {selected_source}"
        
        st.code(final_select, language="sql")


    st.divider() #Add a visual line

    st.subheader("4. Define Target")
    obj_type = st.selectbox("Type", ["Table", "View", "Dynamic Table"])


    if obj_type == "Dynamic Table":
        # Create a form so the app doesn't reload on every keystroke
        with st.form("dt_form"):
            col1, col2 = st.columns(2) # Make it look nice with 2 columns
            
            with col1:
                name_input = st.text_input("Table Name")
                schema_input = st.text_input("Schema", value="SILVER_DB.CLEAN")
                wh_input = st.selectbox("Warehouse", ["COMPUTE_WH", "ETL_WH"])
            
            with col2:
                lag_input = st.text_input("Target Lag", value="1 minute")
            
            # Text area for the SQL Logic
            sql_input = st.text_area("Select Statement (Logic)", height=150)
            
            # The Submit Button
            submitted = st.form_submit_button("Generate DDL")
            
            if submitted:
                # Instantiate a fake dynamictable
                # use the variables from the form (name_input, etc.) 
                new_dt = DynamicTable(
                    name=name_input,
                    schema=schema_input,
                    columns=sql_input,      
                    warehouse=wh_input,     
                    target_lag=lag_input    
                )
                
                st.success("Object generated successfully!")
                st.code(new_dt.create_ddl(), language='sql')


elif selected_layer == "Gold":
    st.header("Gold Layer - Aggregated Facts")
    st.write("This section is under construction.")