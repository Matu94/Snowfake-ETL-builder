import streamlit as st
from utils.snowflake_connector import get_session
from utils.git_manager import push_to_github


def display_deploy_button(ddl_sql,schema_name,object_type,object_name,commitmsg):

    #Renders a 'Deploy' button. When clicked, it executes the provided SQL using the active Snowflake session.
    # Don't show anything if there is no SQL
    if not ddl_sql:
        return
    
    # Using 'type="primary"' makes the button "stand out" - so user will know TO PRESS THIS!
    if st.button("Deploy to Snowflake", type="primary", key="global_deploy_btn"):
        
        session = get_session()
        
        if not session:
            st.error("No active Snowflake connection found. Check your connection settings.")
            return
        
        try:
            with st.spinner("Executing DDL on Snowflake..."):
                # .collect() actually runs the query and returns the result
                result_df = session.sql(ddl_sql).collect()
            
            st.success("Deployment Successful!")
            
            # Show the feedback from Snowflake (e.g. "View TEST_VIEW successfully created.")
            st.dataframe(result_df)
            
            
        except Exception as e:
            st.error(f"Deployment Failed: {e}")

        #Git push
        with st.spinner("Pushing to GitHub..."):
            #Construct a clean path: objects/SCHEMA/TYPE/NAME.sql
            #file_path = f"snowflake_objects/testschema/testtype/testname.sql".lower()
            file_path = f"snowflake_objects/{schema_name}/{object_type}/{object_name}.sql".lower()
            
            git_result = push_to_github(
                file_path=file_path, 
                file_content=ddl_sql, 
                commit_message=commitmsg
            )
            
            if "Success!" in git_result:
                st.success(git_result)
            else:
                st.error(git_result)