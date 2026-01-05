import streamlit as st
import pandas as pd
from utils.snowflake_connector import get_session
from utils.data_provider import get_data_provider
from components.table_editor import create_table
from components.view_editor import create_view
from components.dynamictable_editor import create_dynamic_table
from components.deploy_ui import display_deploy_button


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


    if obj_type == 'Table':
        final_ddl = create_table(target_schema,target_name)
        st.code(final_ddl, language='sql')

    elif obj_type == 'View':
        final_ddl = create_view(editor_source_schema,editor_source_table,target_schema,target_name)
        st.code(final_ddl, language='sql')

    elif obj_type == 'Dynamic Table':
        final_ddl = create_dynamic_table(editor_source_schema,editor_source_table,target_schema,target_name,warehouse,target_lag)
        st.code(final_ddl, language='sql')

    
    st.divider()
    display_deploy_button(final_ddl)

# ==========================================
# PAGE 3: MODIFY EXISTING 
# ==========================================
elif page == "Modify Existing":
    st.header("Modif existing")
    #provider = get_data_provider()

    
# ==========================================
# PAGE 4: Sandbox
# ==========================================
elif page == "Sandbox":
    st.header("Sandbox")
    st.write("This section is my playground")

    provider = get_data_provider()
    st.code(provider.get_schemas(database))
    st.code(provider.get_tables('STAGING'))
    st.code(provider.get_views('STAGING'))
    st.code(provider.get_columns('STAGING','ORDERS_STG','Table'))
    st.code(provider.get_columns('STAGING','V_ORDERS_CLEAN','View'))
    st.warning("Select a valid object type!")


    test = [('ORDER_ID', 'NUMBER(38,0)'), ('USER_ID', 'NUMBER(38,0)'), ('AMOUNT', 'NUMBER(10,2)')]
    for col_name, col_type in test:
        st.info(col_name)
        st.info(col_type)

    st.code(create_table('testschema','testable'), language='sql')
