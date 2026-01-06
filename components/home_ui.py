import streamlit as st
from utils.snowflake_connector import get_session

def home():
    st.markdown("## Home Page")
    st.markdown("""
    Welcome to your **Snowflake Object Builder**. This tool simplifies the Data Engineering lifecycle 
    by allowing you to create, map, and deploy Tables, Views, and Dynamic Tables using a low-code interface.
    """)


    #SOCIALS & LINKS
    #put these in a small row for quick access
    c1, c2, c3 = st.columns([1, 1, 4])
    with c1:
        st.link_button("LinkedIn", "https://www.linkedin.com/in/matu94", type="secondary")
    with c2:
        st.link_button("GitHub", "https://github.com/Matu94", type="secondary")
    
    st.divider()



    #SYSTEM STATUS
    #use a "Dashboard" look
    session = get_session()
    
    if session:
        #Create a container with a border to group these metrics
        with st.container(border=True):
            st.subheader("Connection Status: **Active**")
            
            # 3 Columns for key metrics
            col1, col2, col3 = st.columns(3)
            
            with col1:
                st.metric(label="Current Role", value=session.get_current_role(), delta="Active")
            
            with col2:
                st.metric(label="Warehouse", value=session.get_current_warehouse(), delta="Running", delta_color="off")
            
            with col3:
                st.metric(label="Database", value=session.get_current_database())
            
            st.caption("Environment is healthy and ready for deployment.")

    else:
        # A nice warning card if disconnected
        with st.container(border=True):
            st.error("❌ No active Snowflake session found.")
            st.markdown("Please check your `.streamlit/secrets.toml` configuration or ensure your key pair is valid.")

    st.divider()

    #fancy bullshit to have something in the homepage
    # This guides the user on what to do next
    st.subheader("What would you like to build?")
    
    col_a, col_b, col_c = st.columns(3)

    with col_a:
        with st.container(border=True):
            st.markdown("### Tables")
            st.markdown("Create standard tables from scratch by defining columns and types manually.")
            st.caption("Best for: Raw Data Landing")
    
    with col_b:
        with st.container(border=True):
            st.markdown("### Views")
            st.markdown("Create logical views on top of existing data with simple SQL transformations.")
            st.caption("Best for: Business Logic")

    with col_c:
        with st.container(border=True):
            st.markdown("### Dynamic Tables")
            st.markdown("Build automated declarative pipelines with built-in lag and refresh logic.")
            st.caption("Best for: ETL Pipelines")

    return None