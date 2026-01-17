import streamlit as st
import pandas as pd
from utils.snowflake_connector import get_session
from utils.data_provider import get_data_provider
from components.table_editor import create_table
from components.view_editor import create_view
from components.dynamictable_editor import create_dynamic_table
from components.deploy_ui import display_deploy_button
from components.table_editor import modify_table


session = get_session()
database = session.get_current_database()
provider = get_data_provider()

def create_object():
    st.markdown("### Create new object")
    st.markdown("Configure your new Snowflake object below.")

    #TARGET CONFIGURATION
    with st.container(border=True):
        st.markdown("#### 1. Target Definition")
        
        c1, c2, c3 = st.columns([1, 1, 2]) #Uneven columns for better spacing
        
        with c1:
            obj_type = st.selectbox("Object Type", ["Table", "View", "Dynamic Table"])
        
        with c2:
            target_schema = st.selectbox("Target Schema", provider.get_schemas(database))
        
        with c3:
            target_name = st.text_input("Object Name", placeholder="e.g. CLEAN_USERS_T")


    #SOURCE CONFIGURATION 
    if obj_type in ("Dynamic Table", "View"):
        with st.container(border=True):
            st.markdown("#### 2. Source Data")
            
            c1, c2 = st.columns(2)
            with c1:
                editor_source_schema = st.selectbox("Target Source Schema", provider.get_schemas(database), key="src_schema")
            with c2:
                # We fetch tables based on the schema selected above
                editor_source_table = st.selectbox("Target Source Table", provider.get_tables(editor_source_schema), key="src_table")
            
            st.caption(f"Selecting columns from: **{editor_source_schema}.{editor_source_table}**")


    #ADVANCED SETTINGS
    if obj_type == "Dynamic Table":
        with st.container(border=True):
            st.markdown("#### Dynamic Table Settings")
            
            c1, c2 = st.columns(2)
            with c1:
                warehouse = st.selectbox("Warehouse", ["COMPUTE_WH"], help="WH used for the refresh") #TODO: Get the WHs
            with c2:
                target_lag = st.text_input("Refresh Lag", value="1 minute", help="e.g. '1 minute', '1 hour'")

    st.divider()



    #EDITORS:
    st.subheader(f"Design {obj_type} Columns")
    
    final_ddl = None # Initialize variable

    if obj_type == 'Table':
        # Assumes create_table handles the UI for column editing
        final_ddl = create_table(target_schema, target_name)

    elif obj_type == 'View':
        final_ddl = create_view(editor_source_schema, editor_source_table, target_schema, target_name)

    elif obj_type == 'Dynamic Table':
        final_ddl = create_dynamic_table(editor_source_schema, editor_source_table, target_schema, target_name, warehouse, target_lag)



    # PREVIEW & DEPLOY ---
    if final_ddl:
        st.divider()
        st.markdown("#### Review & Deploy")
        
        st.code(final_ddl, language='sql')
        
        #Deployment Button
        display_deploy_button(final_ddl)
    
    return None




def modify_object():
    st.markdown("### Modify an existing object")
    st.markdown("Configure your Snowflake object below.")

    #TARGET CONFIGURATION
    with st.container(border=True):
        st.markdown("#### 1. Target Definition")
        
        c1, c2, c3 = st.columns([1, 1, 2]) #Uneven columns for better spacing
        
        with c1:
            obj_type = st.selectbox("Object Type", ["Table", "View", "Dynamic Table"])
        
        with c2:
            selected_schema = st.selectbox("Select Schema", provider.get_schemas(database))
        
        with c3:
            if obj_type == "View":
                object_name = st.selectbox("Select Object", provider.get_views(selected_schema))
            elif obj_type == "Table":
                object_name = st.selectbox("Select Object", provider.get_tables(selected_schema, 'normal'))
            elif obj_type == "Dynamic Table":
                object_name = st.selectbox("Select Object", provider.get_tables(selected_schema, 'dynamic'))

    
    
    #EDITORS:
    st.subheader(f"Design {obj_type} Columns")
    
    final_ddl = None # Initialize variable

    if obj_type == 'Table':
        
        final_ddl = modify_table(selected_schema, object_name)

    



    # PREVIEW & DEPLOY ---
    if final_ddl:
        st.divider()
        st.markdown("#### Review & Deploy")
        
        st.code(final_ddl, language='sql')
        
        #Deployment Button
        display_deploy_button(final_ddl)
    
    return None