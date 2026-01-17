import streamlit as st
import pandas as pd
from models.table import Table  
from utils.data_provider import get_data_provider

#Base Types 
sf_types = ["NUMBER", "VARCHAR", "BOOLEAN", "TIMESTAMP", "DATE", "VARIANT", "FLOAT"]
#Base df
default_data = pd.DataFrame(
    [{"col_nm": "ID", "data_type": "NUMBER", "nullable": True}],
)
provider = get_data_provider()



def create_table(target_schema,target_name):

    #1. Create the editor
    # 'options' for data_type only includes standard types
    editor_result = st.data_editor(
        default_data,
        num_rows="dynamic",
        column_config={
            "col_nm": st.column_config.TextColumn("Column Name", required=True),
            "data_type": st.column_config.SelectboxColumn("Data Type", options=sf_types,
                required=True #This tells the data editor that this specific cell cannot be empty
            ),  
            "nullable": st.column_config.CheckboxColumn("Allow Nulls?", default = True),
        },
        use_container_width=True,
        key="table_create_editor" #unique ID badge for this "widget"
    )

    #2. Create the DDL
    col_definitions = []

    for index, row in editor_result.iterrows(): #need index to have string as a result, not tuple
        if row["col_nm"]: 
            col_str = f"{row['col_nm']} {row['data_type']}"
            if not row["nullable"]:
                col_str += " NOT NULL"
            col_definitions.append(col_str)
    cols_sql = ",\n\t".join(col_definitions)          #Result: "ID NUMBER, NAME VARCHAR"
    

    #3. Display the DDL
    result = Table(
        schema = target_schema, 
        name = target_name, 
        columns=cols_sql)


    return result.create_ddl()




def modify_table(selected_schema,selected_object_name):
    #reuse some part from create_table and create_dynamic_table
    #1. Create dynamic col_type options (both standard and already existing)
    #need this because i gave the coice to select the base types, but already existing can have more precies ones like NUMBER(38,0)
    #Fetch ALL columns at once
    rows_list = []
    source_cols = provider.get_columns(selected_schema, selected_object_name, 'Table')
    #Build the rows from source 
    #rows_list is a list, and the result of get_columns is also a list with 2 stuffs in it. first is the column name, second is the type. So with this for loop i can build the required list
    for col_name, col_type, nullable in source_cols:
        rows_list.append({
            "src_col_nm": col_name,
            "data_type": col_type, #This can be 'NUMBER(38,0)', wich is not part of the base types
            "nullable": nullable,
        })

        #Add this specific/more precise type to list if it's not there
        if col_type not in sf_types:
            sf_types.append(col_type)

    
    #2. Create the DataFrame based on existing and base objects
    default_data = pd.DataFrame(rows_list)  


    #3. Create the Editor  
    #Now 'options' includes both standard types and the specific ones fromsource
    editor_result = st.data_editor(
        default_data,
        num_rows="dynamic",
        column_config={
            "src_col_nm": st.column_config.TextColumn("Source Column", required=True),
            "data_type": st.column_config.SelectboxColumn(
                "Data Type", 
                options=sorted(list(set(sf_types))), #set removes duplicates, list converts it back to liust, sorted ofc sort it...
                required=True #This tells the data editor that this specific cell cannot be empty
            ),
            "nullable": st.column_config.CheckboxColumn("Allow Nulls?", default = True),
        },
        use_container_width=True,
        key="table_modify_editor"     #unique ID badge for this 'widget' 
    )   

    #4. Generate DDL   
    col_definitions = []
    
    for index, row in editor_result.iterrows(): #need index to have string as a result, not tuple
        if row["src_col_nm"]: 
            col_str = f"{row['src_col_nm']} {row['data_type']}"
            if row["nullable"] in ('N', 'False'):               #nullable can be 'N/Y' from DESC TABLE command (so init data, first run), or True/False once it was modified in the editor
                col_str += " NOT NULL"
            col_definitions.append(col_str)
    cols_sql = ",\n\t".join(col_definitions)          #Result: "ID NUMBER, NAME VARCHAR"

    #5. Display the DDL
    result = Table(
        schema = selected_schema, 
        name = selected_object_name, 
        columns=cols_sql)
    
    return result.create_ddl()