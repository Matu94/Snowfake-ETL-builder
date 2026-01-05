import streamlit as st
import pandas as pd
from utils.snowflake_connector import get_session
from utils.data_provider import get_data_provider
from models.table import Table  
from models.view import View
from models.dynamic_table import DynamicTable
from components.table_editor import create_table
from components.view_editor import create_view
from components.dynamictable_editor import create_dynamic_table


# ========================================================================================================================================================================
#                                       THIS IS JUST A SANDBOX, SORRY. PLEASE CHECK STREAMLIT_APP.PY
# ========================================================================================================================================================================








#   !!!!!!!!    Page Config     !!!!!!!!
st.set_page_config(page_title="Snowflake Builder", layout="wide")
st.title("❄️ Snowflake Object Builder")

st.sidebar.title("Menu")
page = st.sidebar.radio("Go to", ["Home", "Create New Object", "Modify Existing", "Sandbox"])

session = get_session()
database = session.get_current_database()
provider = get_data_provider()


# ==========================================
# PAGE 1: HOME (Dashboard)
# ==========================================
if page == "Home":
    st.write("### Welcome to the ETL Builder")
    
    # Connection Check
    session = get_session()
    if session:
        st.success(f"Connected to Snowflake! Current Role: {session.get_current_role()}")
        st.info(f"Current Warehouse: {session.get_current_warehouse()}, Current Database: {session.get_current_database()}")
        database = session.get_current_database()
    else:
        st.warning("No active Snowflake session found.")



# ==========================================
# PAGE 2: CREATE NEW OBJECT 
# ==========================================
elif page == "Create New Object":

    st.header("🆕 Create Object from Scratch")
    st.caption("Define your object structure manually by adding columns.")


    ##########################################################  1. Object Settings  ########################################################## 
    with st.container():
        col1, col2, col3 = st.columns(3)
        with col1:
            obj_type = st.selectbox("Select the new object type:", ["Table", "Dynamic Table", "View"])
            target_schema = st.selectbox("Target Schema", provider.get_schemas(database))
            target_name = st.text_input("Object Name", placeholder="e.g. USERS_RAW")
        with col2:
            if obj_type in ("Dynamic Table", "View"):
                editor_source_schema = st.selectbox("Select source schema:", provider.get_schemas(database))
                editor_source_table = st.selectbox("Select source object:", provider.get_tables(editor_source_schema))
        with col3:
            if obj_type == "Dynamic Table":
                target_lag = st.text_input("Target lag:", placeholder="e.g. 10 min")
                warehouse = st.text_input("Warehouse:", placeholder="e.g. COMPUTE_WH")

    st.divider()

    if obj_type == 'View':
        st.code(create_view(editor_source_schema,editor_source_table,target_schema,target_name), language='sql')
    if obj_type == 'Dynamic Table':
        st.code(create_dynamic_table(editor_source_schema,editor_source_table,target_schema,target_name,warehouse,target_lag), language='sql')