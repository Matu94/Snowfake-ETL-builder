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

    st.subheader("4. Review & Configure")

    #use a distinct button to "Lock in and save" the transformation
    if st.button("Generate & Lock SQL"):
        # 1. Re-calculate the SQL (same logic as before)
        select_parts = []
        for index, row in edited_df.iterrows():
            if row["Include"]:
                src = row["Source Column"]
                tgt = row["Target Column Name"]
                logic = row["Transformation"]
                
                if logic:
                    col_sql = f"{logic} AS {tgt}"
                elif src != tgt:
                    col_sql = f"{src} AS {tgt}"
                else:
                    col_sql = src
                select_parts.append(col_sql)
        
        final_select = "SELECT \n    " + ",\n    ".join(select_parts) + f"\nFROM {selected_source}"
        
        #2. SAVE IT TO THE BACKPACK (Session State!!)
        st.session_state['generated_sql'] = final_select
        st.success("SQL Generated! Scroll down to configure deployment.")

    # The Intelligent Form 
    # Only show this form if we have generated SQL in the "backpack"
    if 'generated_sql' in st.session_state:
        
        # Show the SQL being used
        st.info("Using the following logic:")
        st.code(st.session_state['generated_sql'], language="sql")

        st.subheader("4. Deploy Object")
        
        #wrap the creation in a form
        with st.form("deployment_form"):
            col1, col2 = st.columns(2)
            with col1:
                obj_type = st.selectbox("Object Type", ["Dynamic Table", "View", "Table"])
                tgt_schema = st.text_input("Target Schema", value="SILVER_DB.CLEAN")
                tgt_name = st.text_input("Target Name", value=f"CLEAN_{selected_source}")
            
            with col2:
                wh = st.selectbox("Warehouse", ["COMPUTE_WH", "ETL_WH"])
                lag = st.text_input("Target Lag", value="1 minute")

            # The Grand Finale Button
            deploy_clicked = st.form_submit_button("Deploy to Snowflake")

            if deploy_clicked:
                # 1. Instantiate the correct Object based on dropdown
                if obj_type == "Dynamic Table":
                    new_obj = DynamicTable(tgt_name, tgt_schema, st.session_state['generated_sql'], wh, lag)
                elif obj_type == "View":
                    new_obj = View(tgt_name, tgt_schema, st.session_state['generated_sql'])
                else:
                    # Table logic might be different (CTAS), but let's assume CTAS for now
                    new_obj = Table(tgt_name, tgt_schema, st.session_state['generated_sql'])

                # 2. Get the Final DDL
                final_ddl = new_obj.create_ddl()

                # 3. Simulate Execution
                st.toast("Validating Syntax...", icon="🔄")
                
                # real app run: session.sql(final_ddl).collect()
                st.success(f"Successfully created {obj_type}: {tgt_name}")
                st.code(final_ddl, language="sql")
                
                # Optional: Clear state to start over
                # del st.session_state['generated_sql']


elif selected_layer == "Gold":
    st.header("Gold Layer - Aggregated Facts")
    st.write("This section is under construction.")