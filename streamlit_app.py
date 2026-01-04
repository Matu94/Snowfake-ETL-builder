import streamlit as st
import pandas as pd
from utils.snowflake_connector import get_session
from utils.data_provider import get_data_provider
from models.table import Table  
from models.view import View
from models.dynamic_table import DynamicTable
from components.table_editor import create_table


#   !!!!!!!!    Page Config     !!!!!!!!
st.set_page_config(page_title="Snowflake Builder", layout="wide")
st.title("❄️ Snowflake Object Builder")

st.sidebar.title("Menu")
page = st.sidebar.radio("Go to", ["Home", "Create New Object", "Modify Existing", "Sandbox"])

session = get_session()
database = session.get_current_database()



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

    provider = get_data_provider()

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



    ##########################################################  2. Column Definition Editor ########################################################## 
    #need this because i gave the coice to select the base types, but already existing can have more precies ones like NUMBER(38,0)
    st.subheader("Define Columns")

    #Base Types 
    sf_types = ["NUMBER", "VARCHAR", "BOOLEAN", "TIMESTAMP", "DATE", "VARIANT", "FLOAT"]
    
    #Prepare Data & Dynamic Options for existing datatypes from get_columns()
    rows_list = []
    
    #For DT and View:
    if obj_type in ("Dynamic Table", "View"):
        #Fetch ALL columns at once
        source_cols = provider.get_columns(editor_source_schema, editor_source_table, obj_type)
        
        #Build the rows from source 
        #rows_list is a list, and the result of get_columns is also a list with 2 stuffs in it. first is the column name, second is the type. So with this for loop i can build the required list
        for col_name, col_type in source_cols:
            rows_list.append({
                "Column Name": col_name,
                "Transformation Rule": "",
                "Data Type": col_type, #This can be 'NUMBER(38,0)', wich is not part of the base types
                "Nullable": True
            })
            
            #Add this specific/more precise type to list if it's not there
            if col_type not in sf_types:
                sf_types.append(col_type)
    else:
        #Default empty row for "Create from Scratch"
        rows_list.append({"Column Name": "", "Transformation Rule": "", "Data Type": "", "Nullable": True})


    # Create the DataFrame
    default_data = pd.DataFrame(rows_list)



    ##########################################################   3. Create the Editor   ########################################################## 
    #Now 'options' includes both standard types and the specific ones fromsource
    editor_result = st.data_editor(
        default_data,
        num_rows="dynamic",
        column_config={
            "Column Name": st.column_config.TextColumn("Column Name", required=True),
            "Transformation Rule": st.column_config.TextColumn("Transformation Logic (SQL)", help="e.g. UPPER(col)"),
            "Data Type": st.column_config.SelectboxColumn(
                "Data Type", 
                options=sorted(list(set(sf_types))), #set removes duplicates, list converts it back to liust, sorted ofc sort it...
                required=True #This tells the data editor that this specific cell cannot be empty
            ),
            "Nullable": st.column_config.CheckboxColumn("Allow Nulls?", default = True),
        },
        use_container_width=True,
        key="create_editor"
    )

    ##########################################################  4. Generate DDL   ########################################################## 
    col_definitions = []
    col_names_only = [] #For view DDL
    for index, row in editor_result.iterrows(): #need index to have string as a result, not tuple
        if row["Column Name"]: 
            rule = f"{row['Transformation Rule']} AS {row['Column Name']}" if row['Transformation Rule'] else row['Column Name']
            
            #This will now use whatever is in the cell, e.g. "NUMBER(38,0)"
            if obj_type == 'View':
                col_str = f"{rule}::{row['Data Type']}"
            else:
                col_str = f"{rule} {row['Data Type']}"
            
            if not row["Nullable"]:
                col_str += " NOT NULL"
            col_definitions.append(col_str)

            col_names_only.append(row["Column Name"])

    cols_sql = ",\n\t".join(col_definitions)          #Result: "ID NUMBER, NAME VARCHAR"
    cols_names_str = ",\n\t".join(col_names_only)      #Result: "ID, NAME"



    ########################################################## 5. Object display    ##########################################################
    if obj_type == 'Table':
        result = Table(
            schema = target_schema, 
            name = target_name, 
            columns=cols_sql)
        st.code(result.create_ddl(), language='sql')

    if obj_type == 'View':
        result = View(
            schema = target_schema, 
            name = target_name, 
            columns=cols_sql,
            col_names=cols_names_str,
            source_object = f"{editor_source_schema}.{editor_source_table}")
        st.code(result.create_ddl(), language='sql')

    if obj_type == 'Dynamic Table':
        result = DynamicTable(
            schema = target_schema, 
            name = target_name, 
            columns=cols_names_str,
            source_object=f"{editor_source_schema}.{editor_source_table}",
            warehouse=warehouse,
            target_lag=target_lag
        )
        st.code(result.create_ddl(), language='sql')




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