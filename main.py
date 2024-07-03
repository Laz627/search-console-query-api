import streamlit as st
import pandas as pd
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from st_aggrid import AgGrid, GridOptionsBuilder, GridUpdateMode, DataReturnMode
import os

###############################################################################

st.set_page_config(
    layout="wide", page_title="Google Search Console Connector", page_icon="🔌"
)

# Row limit
RowCap = 25000

# Google OAuth configuration
clientSecret = str(st.secrets["installed"]["client_secret"])
clientId = str(st.secrets["installed"]["client_id"])
redirectUri = str(st.secrets["installed"]["redirect_uris"][0])
scopes = ["https://www.googleapis.com/auth/webmasters.readonly"]

# Initialize session state variables
if "credentials" not in st.session_state:
    st.session_state["credentials"] = None

if "token_received" not in st.session_state:
    st.session_state["token_received"] = False

# Google OAuth2.0 Authentication
def get_google_auth_flow():
    return InstalledAppFlow.from_client_config(
        {
            "installed": {
                "client_id": clientId,
                "client_secret": clientSecret,
                "redirect_uris": [redirectUri],
                "auth_uri": "https://accounts.google.com/o/oauth2/auth",
                "token_uri": "https://oauth2.googleapis.com/token",
            }
        },
        scopes=scopes,
    )

def authenticate_user():
    flow = get_google_auth_flow()
    if not st.session_state["token_received"]:
        auth_url, _ = flow.authorization_url(prompt="consent")
        st.markdown(f"[Sign-in with Google]({auth_url})")
    else:
        flow.fetch_token(code=st.session_state["credentials"]["code"])
        credentials = flow.credentials
        st.session_state["credentials"] = credentials

        service = build(
            "webmasters",
            "v3",
            credentials=credentials,
            cache_discovery=False,
        )
        account = searchconsole.account.Account(service, credentials)
        return account

tab1, tab2 = st.tabs(["Main", "About"])

with tab1:
    st.sidebar.image("logo.png", width=290)
    
    # OAuth process
    st.write("### Step 1: Google Authentication")
    if "code" in st.experimental_get_query_params():
        st.session_state["credentials"] = {"code": st.experimental_get_query_params()["code"][0]}
        st.session_state["token_received"] = True

    account = authenticate_user()

    if st.session_state["token_received"]:
        st.write("### Step 2: Fetch Search Console Data")

        site_list = account.service.sites().list().execute()
        site_urls = [site["siteUrl"] for site in site_list["siteEntry"]]

        selected_site = st.selectbox("Select web property", site_urls)

        col1, col2, col3 = st.columns(3)
        with col1:
            dimension = st.selectbox("Dimension", ["query", "page", "date", "device", "searchAppearance", "country"])
        with col2:
            nested_dimension = st.selectbox("Nested dimension", ["none", "query", "page", "date", "device", "searchAppearance", "country"])
        with col3:
            nested_dimension_2 = st.selectbox("Nested dimension 2", ["none", "query", "page", "date", "device", "searchAppearance", "country"])

        col1, col2 = st.columns(2)
        with col1:
            search_type = st.selectbox("Search type", ["web", "video", "image", "news", "googleNews"])
        with col2:
            timescale = st.selectbox("Date range", ["Last 7 days", "Last 30 days", "Last 3 months", "Last 6 months", "Last 12 months", "Last 16 months"])

        # Advanced Filters
        with st.expander("Advanced Filters", expanded=False):
            filter_col1, filter_col2, filter_col3 = st.columns(3)
            with filter_col1:
                filter_page_or_query = st.selectbox("Dimension to filter #1", ["query", "page", "device", "searchAppearance", "country"])
            with filter_col2:
                filter_type = st.selectbox("Filter type", ["contains", "equals", "notContains", "notEquals", "includingRegex", "excludingRegex"])
            with filter_col3:
                filter_keyword = st.text_input("Keyword(s) to filter")

        if st.button("Fetch GSC API data"):
            webproperty = account[selected_site]
            df = webproperty.query.search_type(search_type).range("today", days=timescale).dimension(dimension).filter(filter_page_or_query, filter_keyword, filter_type).limit(RowCap).get().to_dataframe()

            if nested_dimension != "none":
                df = webproperty.query.search_type(search_type).range("today", days=timescale).dimension(dimension, nested_dimension).filter(filter_page_or_query, filter_keyword, filter_type).limit(RowCap).get().to_dataframe()

            if nested_dimension_2 != "none":
                df = webproperty.query.search_type(search_type).range("today", days=timescale).dimension(dimension, nested_dimension, nested_dimension_2).filter(filter_page_or_query, filter_keyword, filter_type).limit(RowCap).get().to_dataframe()

            if df.empty:
                st.warning("No data available for the selected criteria.")
            else:
                st.write(f"Number of results: {len(df)}")
                st.dataframe(df)

                csv = df.to_csv().encode("utf-8")
                st.download_button(label="Download CSV", data=csv, file_name="gsc_data.csv", mime="text/csv")

with tab2:
    st.write("### About this app")
    st.markdown("""
        * One-click connect to the Google Search Console API
        * Easily traverse your account hierarchy
        * Go beyond the 1K row UI limit
        * Enrich your data querying with multiple dimensions and extra filters!
    """)
