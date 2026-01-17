#Base Types 
import streamlit as st
import pandas as pd
from models.view import View  
from utils.data_provider import get_data_provider

#Base Types 
sf_types = ["NUMBER", "VARCHAR", "BOOLEAN", "TIMESTAMP", "DATE", "VARIANT", "FLOAT"]
#Base df
#Prepare Data & Dynamic Options for existing datatypes from get_columns()
provider = get_data_provider()




def create_view(editor_source_schema,editor_source_table,target_schema,target_name):
    
    #1. Create dynamic col_type options (both standard and already existing)
    #need this because i gave the coice to select the base types, but already existing can have more precies ones like NUMBER(38,0)
    #Fetch ALL columns at once
    rows_list = []
    source_cols = provider.get_columns(editor_source_schema, editor_source_table, 'View')
    #Build the rows from source 
    #rows_list is a list, and the result of get_columns is also a list with 2 stuffs in it. first is the column name, second is the type. So with this for loop i can build the required list
    for col_name, col_type, nullable in source_cols:
        rows_list.append({
            "src_col_nm": col_name,
            "new_col_nm": col_name,
            "transformation": "",
            "data_type": col_type #This can be 'NUMBER(38,0)', wich is not part of the base types
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
            "src_col_nm": st.column_config.TextColumn("Source Column", required=True, disabled = True),
            "new_col_nm": st.column_config.TextColumn("New Column Name", required=True),
            "transformation": st.column_config.TextColumn("Transformation", help = "eg. 'LEFT()'"),
            "data_type": st.column_config.SelectboxColumn(
                "Data Type", 
                options=sorted(list(set(sf_types))), #set removes duplicates, list converts it back to liust, sorted ofc sort it...
                required=True #This tells the data editor that this specific cell cannot be empty
            )
        },
        use_container_width=True,
        key="view_create_editor"     #unique ID badge for this 'widget' 
    )   


    #4. Generate DDL   
    col_definitions = []
    col_names_only = [] #For view DDL

    for index, row in editor_result.iterrows(): #need index to have string as a result, not tuple
        if row["src_col_nm"]: 
            
            rule = row['transformation'] if row['transformation'] else row['src_col_nm']    #Check that we have rule or not. if not, use the original column name

            if rule != row["new_col_nm"]:  #If we have rule that means we need to build the string in a different way, we need alias anyways with this method
                col_str = f"{rule}::{row['data_type']} AS {row['new_col_nm']}"
            else:
                col_str = f"{row['src_col_nm']}::{row['data_type']}"
            col_definitions.append(col_str) 
            col_names_only.append(row["new_col_nm"])   
    cols_sql = ",\n\t".join(col_definitions)          #Result: "ID NUMBER, NAME VARCHAR"
    cols_names_str = ",\n\t".join(col_names_only)      #Result: "ID, NAME"  



    #5. Object display  
    result = View(
        schema = target_schema, 
        name = target_name, 
        columns=cols_sql,
        col_names=cols_names_str,
        source_object = f"{editor_source_schema}.{editor_source_table}")
    
    
    return result.create_ddl()



def modify_view(selected_schema,selected_object_name):
    
    #1. Create dynamic col_type options (both standard and already existing)
    #need this because i gave the coice to select the base types, but already existing can have more precies ones like NUMBER(38,0)
    #Fetch ALL columns at once
    rows_list = []
    source_cols = provider.get_columns(selected_schema, selected_object_name, 'View')
    #Build the rows from source 
    #rows_list is a list, and the result of get_columns is also a list with 2 stuffs in it. first is the column name, second is the type. So with this for loop i can build the required list
    for col_name, col_type, nullable in source_cols:
        rows_list.append({
            "src_col_nm": col_name,
            "new_col_nm": col_name,
            "transformation": provider.get_transform_by_alias(selected_schema,selected_object_name,'View',col_name),
            "data_type": col_type #This can be 'NUMBER(38,0)', wich is not part of the base types
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
            "src_col_nm": st.column_config.TextColumn("Source Column", required=True, disabled = True),
            "new_col_nm": st.column_config.TextColumn("New Column Name", required=True),
            "transformation": st.column_config.TextColumn("Transformation", help = "eg. 'LEFT()'"),
            "data_type": st.column_config.SelectboxColumn(
                "Data Type", 
                options=sorted(list(set(sf_types))), #set removes duplicates, list converts it back to liust, sorted ofc sort it...
                required=True #This tells the data editor that this specific cell cannot be empty
            )
        },
        use_container_width=True,
        key="view_create_editor"     #unique ID badge for this 'widget' 
    )   


    #4. Generate DDL   
    col_definitions = []
    col_names_only = [] #For view DDL

    for index, row in editor_result.iterrows(): #need index to have string as a result, not tuple
        if row["src_col_nm"]: 
            
            rule = row['transformation'] if row['transformation'] else row['src_col_nm']    #Check that we have rule or not. if not, use the original column name

            if rule != row["new_col_nm"]:  #If we have rule that means we need to build the string in a different way, we need alias anyways with this method
                col_str = f"{rule}::{row['data_type']} AS {row['new_col_nm']}"
            else:
                col_str = f"{row['src_col_nm']}::{row['data_type']}"
            col_definitions.append(col_str) 
            col_names_only.append(row["new_col_nm"])   
    cols_sql = ",\n\t".join(col_definitions)          #Result: "ID NUMBER, NAME VARCHAR"
    cols_names_str = ",\n\t".join(col_names_only)      #Result: "ID, NAME"  

    #the function returns both, but if i only need one, i can use _ so that will be ignored, like: schemaname, _ = fun()
    source_schema_name, source_obj_name = provider.get_source(selected_schema,selected_object_name,'View')
    source_object = f"{source_schema_name}.{source_obj_name}"


    #5. Object display  
    result = View(
        schema = selected_schema, 
        name = selected_object_name, 
        columns=cols_sql,
        col_names=cols_names_str,
        source_object = source_object)
    
    
    return result.create_ddl()