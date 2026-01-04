import streamlit as st
import pandas as pd
from models.table import Table  

#Base Types 
sf_types = ["NUMBER", "VARCHAR", "BOOLEAN", "TIMESTAMP", "DATE", "VARIANT", "FLOAT"]
#Base df
default_data = pd.DataFrame(
    [{"Column Name": "ID", "Data Type": "NUMBER", "Nullable": True}],
)


##########################################################  1. Create Table ########################################################## 
def create_table(target_schema,target_name):

    #1. Create the editor
    # 'options' for data type only includes standard types
    editor_result = st.data_editor(
        default_data,
        num_rows="dynamic",
        column_config={
            "Column Name": st.column_config.TextColumn("Column Name", required=True),
            "Data Type": st.column_config.SelectboxColumn(options=sf_types,
                required=True #This tells the data editor that this specific cell cannot be empty
            ),
            "Nullable": st.column_config.CheckboxColumn("Allow Nulls?", default = True),
        },
        use_container_width=True,
        key="create_editor"
    )

    #2. Create the DDL
    col_definitions = []

    for index, row in editor_result.iterrows(): #need index to have string as a result, not tuple
        if row["Column Name"]: 
            col_str = f"{row['Column Name']} {row['Data Type']}"
            if not row["Nullable"]:
                col_str += " NOT NULL"
            col_definitions.append(col_str)
    cols_sql = ",\n\t".join(col_definitions)          #Result: "ID NUMBER, NAME VARCHAR"
    

    #3. Display the DDL
    result = Table(
        schema = target_schema, 
        name = target_name, 
        columns=cols_sql)


    return result.create_ddl()
