import streamlit as st
import pandas as pd
from utils.snowflake_connector import get_session
from utils.data_provider import get_data_provider
from components.builders_ui import create_object
from components.builders_ui import modify_object
from components.home_ui import home



#   !!!!!!!!    Page Config     !!!!!!!!
st.set_page_config(page_title="Igloo", layout="wide")
st.title("❄️Igloo - Snowflake Object Management Tool")
st.divider()

st.sidebar.title("Menu")
page = st.sidebar.radio("Go to", ["Home", "Create New Object", "Modify Existing", "Sandbox"])

session = get_session()
database = session.get_current_database()
provider = get_data_provider()


# ==========================================
# PAGE 1: HOME (Dashboard)
# ==========================================
if page == "Home":
    home()


# ==========================================
# PAGE 2: CREATE NEW OBJECT 
# ==========================================
elif page == "Create New Object":
    create_object()


# ==========================================
# PAGE 3: MODIFY EXISTING 
# ==========================================
elif page == "Modify Existing":
    modify_object()


    
# ==========================================
# PAGE 4: Sandbox
# ==========================================
elif page == "Sandbox":
    st.header("Sandbox")
    st.write("This section is my playground")

    tf = provider.get_transform('ANALYTICS','NEWVIEW','View')
    st.code(tf)
    st.code(provider.get_transform_by_alias('ANALYTICS','NEWVIEW','View','ID')) 
    
    st.divider()
    st.code(provider.get_transform('ANALYTICS','testdt','Dynamic Table'))
    st.code(provider.get_transform('ANALYTICS','testdt','Dynamic Table')[0]['transformation'])
    st.code(provider.get_transform_by_alias('ANALYTICS','testdt','Dynamic Table','ID')),

