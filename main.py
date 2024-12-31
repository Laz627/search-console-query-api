# Standard library imports
import datetime
import base64
import io

# Related third-party imports
import streamlit as st
from google_auth_oauthlib.flow import Flow
from googleapiclient.discovery import build
import pandas as pd
import searchconsole

# Configuration: Set to True if running locally, False if running on Streamlit Cloud
IS_LOCAL = False

# Constants
SEARCH_TYPES = ["web", "image", "video", "news", "discover", "googleNews"]
DATE_RANGE_OPTIONS = [
    "Last 7 Days",
    "Last 30 Days",
    "Last 3 Months",
    "Last 6 Months",
    "Last 12 Months",
    "Last 16 Months",
    "Custom Range"
]
DEVICE_OPTIONS = ["All Devices", "desktop", "mobile", "tablet"]
BASE_DIMENSIONS = ["page", "query", "country", "date"]
MAX_ROWS = 250_000
DF_PREVIEW_ROWS = 100

# -------------
# Streamlit App Configuration
# -------------

def setup_streamlit():
    st.set_page_config(page_title="Google Search Console API Connector", layout="wide")
    st.title("Google Search Console API Connector")
    st.subheader("Export Up To 250,000 Keywords Seamlessly")
    st.markdown("By: Brandon Lazovic")

    st.markdown("""
    ### Instructions
    1. Sign in with your Google account.
    2. Select a Search Console property.
    3. Choose the desired search type and date range.
    4. Optionally, apply keyword or URL filters.
    5. Click "Fetch Data" to retrieve the data.
    6. Optionally, compare data between different time periods.
    7. Download the results as a CSV file.
    """)

def init_session_state():
    if 'selected_property' not in st.session_state:
        st.session_state.selected_property = None
    if 'selected_search_type' not in st.session_state:
        st.session_state.selected_search_type = 'web'
    if 'selected_date_range' not in st.session_state:
        st.session_state.selected_date_range = 'Last 7 Days'
    if 'start_date' not in st.session_state:
        st.session_state.start_date = datetime.date.today() - datetime.timedelta(days=7)
    if 'end_date' not in st.session_state:
        st.session_state.end_date = datetime.date.today()
    if 'selected_dimensions' not in st.session_state:
        st.session_state.selected_dimensions = ['page', 'query']
    if 'selected_device' not in st.session_state:
        st.session_state.selected_device = 'All Devices'
    if 'custom_start_date' not in st.session_state:
        st.session_state.custom_start_date = datetime.date.today() - datetime.timedelta(days=7)
    if 'custom_end_date' not in st.session_state:
        st.session_state.custom_end_date = datetime.date.today()
    if 'filter_keywords' not in st.session_state:
        st.session_state.filter_keywords = ''
    if 'filter_keywords_not' not in st.session_state:
        st.session_state.filter_keywords_not = ''
    if 'filter_url' not in st.session_state:
        st.session_state.filter_url = ''
    if 'compare' not in st.session_state:
        st.session_state.compare = False
    if 'compare_start_date' not in st.session_state:
        st.session_state.compare_start_date = datetime.date.today() - datetime.timedelta(days=14)
    if 'compare_end_date' not in st.session_state:
        st.session_state.compare_end_date = datetime.date.today() - datetime.timedelta(days=7)

# -------------
# Google Authentication Functions
# -------------

def load_config():
    client_config = {
        "installed": {
            "client_id": st.secrets["oauth"]["client_id"],
            "client_secret": st.secrets["oauth"]["client_secret"],
            "auth_uri": "https://accounts.google.com/o/oauth2/auth",
            "token_uri": "https://accounts.google.com/o/oauth2/token",
            "redirect_uris": (
                ["http://localhost:8501"]
                if IS_LOCAL
                else ["https://search-console-query-api.streamlit.app"]
            ),
        }
    }
    return client_config

def init_oauth_flow(client_config):
    scopes = ["https://www.googleapis.com/auth/webmasters.readonly"]
    return Flow.from_client_config(
        client_config,
        scopes=scopes,
        redirect_uri=client_config["installed"]["redirect_uris"][0],
    )

def google_auth(client_config):
    flow = init_oauth_flow(client_config)
    auth_url, _ = flow.authorization_url(prompt="consent")
    return flow, auth_url

def auth_search_console(client_config, credentials):
    token = {
        "token": credentials.token,
        "refresh_token": credentials.refresh_token,
        "token_uri": credentials.token_uri,
        "client_id": credentials.client_id,
        "client_secret": credentials.client_secret,
        "scopes": credentials.scopes,
        "id_token": getattr(credentials, "id_token", None),
    }
    return searchconsole.authenticate(client_config=client_config, credentials=token)

# -------------
# Data Fetching Functions
# -------------

def fetch_gsc_data(webproperty, search_type, start_date, end_date, dimensions, device_type=None, filter_keywords=None, filter_keywords_not=None, filter_url=None, progress=None):
    query = webproperty.query.range(start_date, end_date).search_type(search_type).dimension(*dimensions)

    if 'device' in dimensions and device_type and device_type != 'All Devices':
        query = query.filter('device', 'equals', device_type.lower())

    st.write("Fetching data...")
    progress.progress(0.2)

    try:
        df = query.limit(MAX_ROWS).get().to_dataframe()
        st.write("Data fetched.")
        progress.progress(0.4)

        if filter_keywords:
            st.write("Applying keyword filter (contains)...")
            keywords = [kw.strip() for kw in filter_keywords.split(',')]
            df = df[df['query'].str.contains('|'.join(keywords), case=False, na=False)]
            progress.progress(0.6)

        if filter_keywords_not:
            st.write("Applying keyword filter (does not contain)...")
            keywords_not = [kw.strip() for kw in filter_keywords_not.split(',')]
            for keyword in keywords_not:
                df = df[~df['query'].str.contains(keyword, case=False, na=False)]
            progress.progress(0.8)

        if filter_url:
            st.write("Applying URL filter...")
            df = df[df['page'].str.contains(filter_url, case=False, na=False)]

        st.write("Data filtering complete.")
        df.reset_index(drop=True, inplace=True)  # Reset the index before returning the DataFrame
        progress.progress(1.0)
        return df
    except Exception as e:
        progress.progress(1.0)
        show_error(e)
        return pd.DataFrame()

def fetch_compare_data(webproperty, search_type, compare_start_date, compare_end_date, dimensions, device_type=None):
    query = webproperty.query.range(compare_start_date, compare_end_date).search_type(search_type).dimension(*dimensions)

    if 'device' in dimensions and device_type and device_type != 'All Devices':
        query = query.filter('device', 'equals', device_type.lower())

    st.write("Fetching comparison data...")
    progress = st.progress(0.5)

    try:
        df = query.limit(MAX_ROWS).get().to_dataframe()
        df.reset_index(drop=True, inplace=True)  # Reset the index before returning the DataFrame
        st.write("Comparison data fetched.")
        progress.progress(1.0)
        return df
    except Exception as e:
        progress.progress(1.0)
        show_error(e)
        return pd.DataFrame()

# -------------
# Utility Functions
# -------------

def update_dimensions(selected_search_type):
    return BASE_DIMENSIONS + ['device'] if selected_search_type in SEARCH_TYPES else BASE_DIMENSIONS

def calc_date_range(selection, custom_start=None, custom_end=None):
    range_map = {
        'Last 7 Days': 7,
        'Last 30 Days': 30,
        'Last 3 Months': 90,
        'Last 6 Months': 180,
        'Last 12 Months': 365,
        'Last 16 Months': 480
    }
    today = datetime.date.today()
    if selection == 'Custom Range':
        if custom_start and custom_end:
            return custom_start, custom_end
        else:
            return today - datetime.timedelta(days=7), today
    return today - datetime.timedelta(days=range_map.get(selection, 0)), today

def show_error(e):
    st.error(f"An error occurred: {e}")

def property_change():
    st.session_state.selected_property = st.session_state['selected_property_selector']

def list_gsc_properties(credentials):
    service = build('webmasters', 'v3', credentials=credentials)
    site_list = service.sites().list().execute()
    return [site['siteUrl'] for site in site_list.get('siteEntry', [])] or ["No properties found"]

# -------------
# File & Download Operations
# -------------

def show_dataframe(report):
    with st.expander("Preview the First 100 Rows"):
        st.dataframe(report.head(DF_PREVIEW_ROWS))

def download_csv_link(report):
    try:
        # Reset the DataFrame index before converting to CSV
        report.reset_index(drop=True, inplace=True)

        def to_csv(df):
            return df.to_csv(index=False, encoding='utf-8-sig')

        csv = to_csv(report)
        b64_csv = base64.b64encode(csv.encode()).decode()
        href = f'<a href="data:file/csv;base64,{b64_csv}" download="search_console_data.csv">Download CSV File</a>'
        st.markdown(href, unsafe_allow_html=True)
    except Exception as e:
        st.error(f"Error converting DataFrame to CSV: {e}")

# -------------
# Streamlit UI Components
# -------------

def show_google_sign_in(auth_url):
    with st.sidebar:
        if st.button("Sign in with Google"):
            st.write('Please click the link below to sign in:')
            st.markdown(f'[Google Sign-In]({auth_url})', unsafe_allow_html=True)

def show_property_selector(properties, account):
    selected_property = st.selectbox(
        "Select a Search Console Property:",
        properties,
        index=properties.index(
            st.session_state.selected_property) if st.session_state.selected_property in properties else 0,
        key='selected_property_selector',
        on_change=property_change
    )
    return account[selected_property]

def show_search_type_selector():
    return st.selectbox(
        "Select Search Type:",
        SEARCH_TYPES,
        index=SEARCH_TYPES.index(st.session_state.selected_search_type),
        key='search_type_selector'
    )

def show_date_range_selector():
    return st.selectbox(
        "Select Date Range:",
        DATE_RANGE_OPTIONS,
        index=DATE_RANGE_OPTIONS.index(st.session_state.selected_date_range),
        key='date_range_selector'
    )

def show_custom_date_inputs():
    st.session_state.custom_start_date = st.date_input("Start Date", st.session_state.custom_start_date)
    st.session_state.custom_end_date = st.date_input("End Date", st.session_state.custom_end_date)

def show_dimensions_selector(search_type):
    available_dimensions = update_dimensions(search_type)
    return st.multiselect(
        "Select Dimensions:",
        available_dimensions,
        default=st.session_state.selected_dimensions,
        key='dimensions_selector'
    )

def show_fetch_data_button(webproperty, search_type, start_date, end_date, selected_dimensions, filter_keywords, filter_keywords_not, filter_url):
    if st.button("Fetch Data"):
        progress = st.progress(0)
        if st.session_state.compare:
            compare_start_date = st.session_state.compare_start_date
            compare_end_date = st.session_state.compare_end_date
            compare_report = fetch_compare_data(webproperty, search_type, compare_start_date, compare_end_date, selected_dimensions, st.session_state.selected_device)

            if compare_report is not None and not compare_report.empty:
                st.write("### Comparison data fetched successfully!")
                report = fetch_gsc_data(webproperty, search_type, start_date, end_date, selected_dimensions, st.session_state.selected_device, st.session_state.filter_keywords, st.session_state.filter_keywords_not, st.session_state.filter_url, progress)
                merged_report = compare_data(report, compare_report)
                show_dataframe(merged_report)
                download_csv_link(merged_report)
            else:
                st.write("No comparison data found for the selected parameters.")
        else:
            report = fetch_gsc_data(webproperty, search_type, start_date, end_date, selected_dimensions, st.session_state.selected_device, st.session_state.filter_keywords, st.session_state.filter_keywords_not, st.session_state.filter_url, progress)

            if report is not None and not report.empty:
                st.write("### Data fetched successfully!")
                show_dataframe(report)
                download_csv_link(report)
            else:
                st.write("No data found for the selected parameters.")

def show_comparison_option():
    if 'compare' not in st.session_state:
        st.session_state.compare = False
    st.session_state.compare = st.checkbox("Compare Time Periods", value=st.session_state.compare)

    if st.session_state.compare:
        if 'compare_start_date' not in st.session_state:
            st.session_state.compare_start_date = datetime.date.today() - datetime.timedelta(days=14)
        if 'compare_end_date' not in st.session_state:
            st.session_state.compare_end_date = datetime.date.today() - datetime.timedelta(days=7)
        st.session_state.compare_start_date = st.date_input("Comparison Start Date", st.session_state.compare_start_date)
        st.session_state.compare_end_date = st.date_input("Comparison End Date", st.session_state.compare_end_date)

def show_filter_options():
    st.session_state.filter_keywords = st.text_input("Keyword Filter (contains, separate multiple with commas)")
    st.session_state.filter_keywords_not = st.text_input("Keyword Filter (does not contain, separate multiple with commas)")
    st.session_state.filter_url = st.text_input("URL or Subfolder Filter (contains)")

def compare_data(report, compare_report):
    merged_report = report.merge(compare_report, on=['page', 'query'], suffixes=('_current', '_compare'))
    merged_report['clicks_diff'] = merged_report['clicks_current'] - merged_report['clicks_compare']
    merged_report['impressions_diff'] = merged_report['impressions_current'] - merged_report['impressions_compare']
    return merged_report

# -------------
# Main Streamlit App Function
# -------------

def main():
    setup_streamlit()
    client_config = load_config()
    st.session_state.auth_flow, st.session_state.auth_url = google_auth(client_config)

    query_params = st.query_params
    auth_code = query_params.get("code", [None])[0]

    if auth_code and not st.session_state.get('credentials'):
        st.session_state.auth_flow.fetch_token(code=auth_code)
        st.session_state.credentials = st.session_state.auth_flow.credentials

    if not st.session_state.get('credentials'):
        show_google_sign_in(st.session_state.auth_url)
    else:
        init_session_state()
        account = auth_search_console(client_config, st.session_state.credentials)
        properties = list_gsc_properties(st.session_state.credentials)

        if properties:
            webproperty = show_property_selector(properties, account)
            search_type = show_search_type_selector()
            date_range_selection = show_date_range_selector()

            if date_range_selection == 'Custom Range':
                show_custom_date_inputs()
                start_date, end_date = st.session_state.custom_start_date, st.session_state.custom_end_date
            else:
                start_date, end_date = calc_date_range(date_range_selection)

            selected_dimensions = show_dimensions_selector(search_type)
            show_comparison_option()
            show_filter_options()

            show_fetch_data_button(webproperty, search_type, start_date, end_date, selected_dimensions, st.session_state.filter_keywords, st.session_state.filter_keywords_not, st.session_state.filter_url)

if __name__ == "__main__":
    main()
