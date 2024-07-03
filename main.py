import streamlit as st
import pandas as pd
from google_auth_oauthlib.flow import Flow
from googleapiclient.discovery import build

# Set page configuration
st.set_page_config(layout="wide", page_title="Google Search Console Connector", page_icon="🔌")

# Row limit
RowCap = 25000

# Initialize session state variables
if "credentials" not in st.session_state:
    st.session_state["credentials"] = None

if "token_received" not in st.session_state:
    st.session_state["token_received"] = False

if "client_id" not in st.session_state:
    st.session_state["client_id"] = ""

if "client_secret" not in st.session_state:
    st.session_state["client_secret"] = ""

if "redirect_uri" not in st.session_state:
    st.session_state["redirect_uri"] = ""

if "credentials_saved" not in st.session_state:
    st.session_state["credentials_saved"] = False

# Google OAuth2.0 Authentication
def get_google_auth_flow(client_id, client_secret, redirect_uri):
    return Flow.from_client_config(
        {
            "installed": {
                "client_id": client_id,
                "client_secret": client_secret,
                "redirect_uris": [redirect_uri],
                "auth_uri": "https://accounts.google.com/o/oauth2/auth",
                "token_uri": "https://oauth2.googleapis.com/token",
            }
        },
        scopes=["https://www.googleapis.com/auth/webmasters.readonly"]
    )

def authenticate_user(client_id, client_secret, redirect_uri):
    flow = get_google_auth_flow(client_id, client_secret, redirect_uri)
    auth_url, _ = flow.authorization_url(prompt="consent", redirect_uri=redirect_uri)
    st.experimental_set_query_params(auth_url=auth_url)
    st.markdown(f"[Sign-in with Google]({auth_url})")
    return flow

def fetch_token(flow, code):
    try:
        flow.fetch_token(code=code, redirect_uri=st.session_state["redirect_uri"])
        credentials = flow.credentials
        st.session_state["credentials"] = credentials
        st.session_state["token_received"] = True
        return build("webmasters", "v3", credentials=credentials, cache_discovery=False)
    except Exception as e:
        st.error(f"An error occurred during authentication: {e}")
        st.session_state["token_received"] = False

# User inputs for OAuth credentials
st.write("### Enter Your Google OAuth Credentials")
st.session_state["client_id"] = st.text_input("Client ID", type="password", value=st.session_state["client_id"])
st.session_state["client_secret"] = st.text_input("Client Secret", type="password", value=st.session_state["client_secret"])
st.session_state["redirect_uri"] = st.text_input("Redirect URI", value="https://your-app-name.streamlit.app" if st.session_state["redirect_uri"] == "" else st.session_state["redirect_uri"])  # Update to your default or user's URL

if st.button("Save Credentials"):
    st.session_state["credentials_saved"] = True

if st.session_state["credentials_saved"]:
    st.write("### Step 1: Google Authentication")
    if "code" in st.experimental_get_query_params():
        st.session_state["token_received"] = True
        code = st.experimental_get_query_params()["code"][0]
        flow = get_google_auth_flow(st.session_state["client_id"], st.session_state["client_secret"], st.session_state["redirect_uri"])
        service = fetch_token(flow, code)
    else:
        service = authenticate_user(st.session_state["client_id"], st.session_state["client_secret"], st.session_state["redirect_uri"])

    if st.session_state["token_received"]:
        st.write("### Step 2: Fetch Search Console Data")

        try:
            site_list = service.sites().list().execute()
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
                request = {
                    'startDate': '2022-01-01',
                    'endDate': '2022-12-31',
                    'dimensions': [dimension],
                    'searchType': search_type,
                    'rowLimit': RowCap,
                    'dimensionFilterGroups': [{
                        'filters': [{
                            'dimension': filter_page_or_query,
                            'operator': filter_type,
                            'expression': filter_keyword
                        }]
                    }]
                }

                if nested_dimension != "none":
                    request['dimensions'].append(nested_dimension)

                if nested_dimension_2 != "none":
                    request['dimensions'].append(nested_dimension_2)

                response = service.searchanalytics().query(siteUrl=selected_site, body=request).execute()
                rows = response.get('rows', [])

                if not rows:
                    st.warning("No data available for the selected criteria.")
                else:
                    df = pd.DataFrame.from_records([row['keys'] + [row['clicks'], row['impressions'], row['ctr'], row['position']] for row in rows],
                                                    columns=(request['dimensions'] + ['Clicks', 'Impressions', 'CTR', 'Position']))
                    st.write(f"Number of results: {len(df)}")
                    st.dataframe(df)

                    csv = df.to_csv().encode("utf-8")
                    st.download_button(label="Download CSV", data=csv, file_name="gsc_data.csv", mime="text/csv")
        except Exception as e:
            st.error(f"An error occurred: {e}")

st.write("### About this app")
st.markdown("""
    * One-click connect to the Google Search Console API
    * Easily traverse your account hierarchy
    * Go beyond the 1K row UI limit
    * Enrich your data querying with multiple dimensions and extra filters!
""")
