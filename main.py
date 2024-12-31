import datetime
import base64
import io
import re
import urllib.parse
import streamlit as st
from google_auth_oauthlib.flow import Flow
from googleapiclient.discovery import build
import pandas as pd
import searchconsole

IS_LOCAL = False
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
BASE_DIMENSIONS = ["page", "query", "country", "date"]
MAX_ROWS = 250_000
DF_PREVIEW_ROWS = 100

###############################################################################
# 1) Handle code truncation in older Streamlit with st.experimental_get_query_params()
###############################################################################

def reassemble_auth_code(params):
    """
    Safely extract the 'code' parameter from st.experimental_get_query_params(),
    and rejoin the slash if it got split.
    """
    code_list = params.get("code")
    if not code_list:
        return None

    code_val = code_list[0]
    if not code_val:
        return None

    # If the code_val is "4" or ends with "/", attempt to rejoin
    if code_val == "4" or code_val.endswith("/"):
        leftover_key = None
        for k in params.keys():
            if k not in ["code", "state", "scope"]:
                leftover_key = k
                break
        if leftover_key:
            code_val = code_val.rstrip("/") + "/" + leftover_key.lstrip("/")
    return code_val

###############################################################################
# 2) Streamlit page setup & session state init
###############################################################################

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
    if "selected_property" not in st.session_state:
        st.session_state.selected_property = None
    if "selected_search_type" not in st.session_state:
        st.session_state.selected_search_type = "web"
    if "selected_date_range" not in st.session_state:
        st.session_state.selected_date_range = "Last 7 Days"
    if "start_date" not in st.session_state:
        st.session_state.start_date = datetime.date.today() - datetime.timedelta(days=7)
    if "end_date" not in st.session_state:
        st.session_state.end_date = datetime.date.today()
    if "selected_dimensions" not in st.session_state:
        st.session_state.selected_dimensions = ["page", "query"]
    if "selected_device" not in st.session_state:
        st.session_state.selected_device = "All Devices"
    if "custom_start_date" not in st.session_state:
        st.session_state.custom_start_date = datetime.date.today() - datetime.timedelta(days=7)
    if "custom_end_date" not in st.session_state:
        st.session_state.custom_end_date = datetime.date.today()
    if "filter_keywords" not in st.session_state:
        st.session_state.filter_keywords = ""
    if "filter_keywords_not" not in st.session_state:
        st.session_state.filter_keywords_not = ""
    if "filter_url" not in st.session_state:
        st.session_state.filter_url = ""
    if "compare" not in st.session_state:
        st.session_state.compare = False
    if "compare_start_date" not in st.session_state:
        st.session_state.compare_start_date = datetime.date.today() - datetime.timedelta(days=14)
    if "compare_end_date" not in st.session_state:
        st.session_state.compare_end_date = datetime.date.today() - datetime.timedelta(days=7)

###############################################################################
# 3) Google OAuth flow setup & GSC property listing
###############################################################################

def load_config():
    client_config = {
        "web": {
            "client_id": st.secrets["oauth"]["client_id"],
            "client_secret": st.secrets["oauth"]["client_secret"],
            "auth_uri": "https://accounts.google.com/o/oauth2/auth",
            "token_uri": "https://oauth2.googleapis.com/token",
            "redirect_uris": (
                ["http://localhost:8501"] if IS_LOCAL
                else ["https://search-console-query-api.streamlit.app"]
                # Must EXACTLY match what's in your Google Cloud Console
            ),
        }
    }
    return client_config

def init_oauth_flow(client_config):
    scopes = ["https://www.googleapis.com/auth/webmasters.readonly"]
    redirect_uri = client_config["web"]["redirect_uris"][0]
    flow = Flow.from_client_config(
        client_config,
        scopes=scopes,
        redirect_uri=redirect_uri
    )
    return flow

def google_auth(client_config):
    flow = init_oauth_flow(client_config)
    auth_url, _ = flow.authorization_url(
        prompt="consent",
        access_type="offline",
        include_granted_scopes="true"
    )
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

def show_google_sign_in(auth_url):
    with st.sidebar:
        if st.button("Sign in with Google"):
            st.write("Please click the link below to sign in:")
            st.markdown(f"[Google Sign-In]({auth_url})", unsafe_allow_html=True)

def list_gsc_properties(credentials):
    service = build("webmasters", "v3", credentials=credentials)
    site_list = service.sites().list().execute()
    return [site["siteUrl"] for site in site_list.get("siteEntry", [])] or ["No properties found"]

###############################################################################
# 4) Search Console data fetching (chunked, sequential)
###############################################################################

def _fetch_chunk(
    webproperty,
    search_type,
    dimensions,
    device_type,
    chunk_start,
    chunk_end,
    filter_url=None
):
    """
    Helper for a single chunk. 
    Note the page filter applied (if filter_url is present).
    """
    st.write(f"[DEBUG] Fetching chunk {chunk_start} -> {chunk_end}")
    query = (
        webproperty.query
        .range(chunk_start, chunk_end)
        .search_type(search_type)
        .dimension(*dimensions)
    )

    # Device filter if selected
    if "device" in dimensions and device_type and device_type != "All Devices":
        query = query.filter("device", "equals", device_type.lower())

    # Apply the subfolder filter at the API level to avoid missing data
    if filter_url:
        query = query.filter("page", "includingRegex", "/features-options/")

    df_chunk = query.limit(250000).get().to_dataframe()
    df_chunk.reset_index(drop=True, inplace=True)
    st.write(f"[DEBUG] Got {len(df_chunk)} rows for chunk {chunk_start}->{chunk_end}")
    return df_chunk

def fetch_gsc_data_in_chunks(
    webproperty,
    search_type,
    start_date,
    end_date,
    dimensions,
    device_type=None,
    filter_keywords=None,
    filter_keywords_not=None,
    filter_url=None
):
    """
    Fetches data in ~3-month (90-day) increments sequentially,
    combining into a single DataFrame. The subfolder (page) filter
    is applied in _fetch_chunk to ensure no missing data.
    """
    st.write("**Info:** Fetching data in smaller chunks (sequential).")

    chunk_size_days = 90
    current_start = start_date
    results = []

    while current_start <= end_date:
        current_end = current_start + datetime.timedelta(days=chunk_size_days - 1)
        if current_end > end_date:
            current_end = end_date

        try:
            df_chunk = _fetch_chunk(
                webproperty,
                search_type,
                dimensions,
                device_type,
                current_start,
                current_end,
                filter_url=filter_url
            )
            results.append(df_chunk)
        except Exception as e:
            st.write(f"[ERROR] Chunk {current_start}->{current_end} failed: {e}")

        current_start = current_end + datetime.timedelta(days=1)

    # Combine all chunks
    if results:
        df_all = pd.concat(results, ignore_index=True)
        # Remove duplicates if you suspect overlap
        df_all.drop_duplicates(
            subset=dimensions + ["clicks", "impressions", "ctr", "position"],
            inplace=True
        )
    else:
        df_all = pd.DataFrame()

    # Apply final filters for keywords (query contains) if needed
    if not df_all.empty:
        if filter_keywords:
            keywords = [kw.strip() for kw in filter_keywords.split(",")]
            # OR logic for keywords
            df_all = df_all[df_all["query"].str.contains("|".join(keywords), case=False, na=False)]
        if filter_keywords_not:
            for kw_not in filter_keywords_not.split(","):
                df_all = df_all[~df_all["query"].str.contains(kw_not.strip(), case=False, na=False)]

        # NOTE: We removed the old final filter for filter_url because we now
        # filter at the query level to avoid data loss.

    return df_all

def fetch_compare_data(webproperty, search_type, compare_start_date, compare_end_date, dimensions, device_type=None):
    """
    Comparison is done with a single 250k-limit call (not chunked).
    """
    st.write("Fetching comparison data (single query).")
    progress = st.progress(0.5)
    query = webproperty.query.range(compare_start_date, compare_end_date).search_type(search_type).dimension(*dimensions)

    if "device" in dimensions and device_type and device_type != "All Devices":
        query = query.filter("device", "equals", device_type.lower())

    try:
        df = query.limit(250000).get().to_dataframe()
        df.reset_index(drop=True, inplace=True)
        st.write("Comparison data fetched.")
        progress.progress(1.0)
        return df
    except Exception as e:
        st.error(f"Comparison fetch error: {e}")
        progress.progress(1.0)
        return pd.DataFrame()

###############################################################################
# 5) UI and final bits
###############################################################################

def property_change():
    st.session_state.selected_property = st.session_state["selected_property_selector"]

def show_property_selector(properties, account):
    selected_property = st.selectbox(
        "Select a Search Console Property:",
        properties,
        index=properties.index(st.session_state.selected_property)
        if st.session_state.selected_property in properties else 0,
        key="selected_property_selector",
        on_change=property_change
    )
    return account[selected_property]

def update_dimensions(selected_search_type):
    return BASE_DIMENSIONS + ["device"] if selected_search_type in SEARCH_TYPES else BASE_DIMENSIONS

def show_search_type_selector():
    return st.selectbox(
        "Select Search Type:",
        SEARCH_TYPES,
        index=SEARCH_TYPES.index(st.session_state.selected_search_type),
        key="search_type_selector"
    )

def show_date_range_selector():
    return st.selectbox(
        "Select Date Range:",
        DATE_RANGE_OPTIONS,
        index=DATE_RANGE_OPTIONS.index(st.session_state.selected_date_range),
        key="date_range_selector"
    )

def show_custom_date_inputs():
    st.session_state.custom_start_date = st.date_input("Start Date", st.session_state.custom_start_date)
    st.session_state.custom_end_date = st.date_input("End Date", st.session_state.custom_end_date)

def calc_date_range(selection, custom_start=None, custom_end=None):
    range_map = {
        "Last 7 Days": 7,
        "Last 30 Days": 30,
        "Last 3 Months": 90,
        "Last 6 Months": 180,
        "Last 12 Months": 365,
        "Last 16 Months": 480
    }
    today = datetime.date.today()
    if selection == "Custom Range":
        if custom_start and custom_end:
            return custom_start, custom_end
        else:
            return today - datetime.timedelta(days=7), today
    return today - datetime.timedelta(days=range_map.get(selection, 0)), today

def show_error(e):
    st.error(f"An error occurred: {e}")

def show_comparison_option():
    if "compare" not in st.session_state:
        st.session_state.compare = False
    st.session_state.compare = st.checkbox("Compare Time Periods", value=st.session_state.compare)

    if st.session_state.compare:
        if "compare_start_date" not in st.session_state:
            st.session_state.compare_start_date = datetime.date.today() - datetime.timedelta(days=14)
        if "compare_end_date" not in st.session_state:
            st.session_state.compare_end_date = datetime.date.today() - datetime.timedelta(days=7)
        st.session_state.compare_start_date = st.date_input("Comparison Start Date", st.session_state.compare_start_date)
        st.session_state.compare_end_date = st.date_input("Comparison End Date", st.session_state.compare_end_date)

def show_filter_options():
    st.session_state.filter_keywords = st.text_input("Keyword Filter (contains, separate multiple with commas)")
    st.session_state.filter_keywords_not = st.text_input("Keyword Filter (does not contain, separate multiple with commas)")
    st.session_state.filter_url = st.text_input("URL or Subfolder Filter (contains)")

def show_dataframe(report):
    with st.expander("Preview the First 100 Rows"):
        st.dataframe(report.head(100))

def download_csv_link(report):
    try:
        report.reset_index(drop=True, inplace=True)

        def to_csv(df):
            return df.to_csv(index=False, encoding="utf-8-sig")

        csv = to_csv(report)
        b64_csv = base64.b64encode(csv.encode()).decode()
        href = f'<a href="data:file/csv;base64,{b64_csv}" download="search_console_data.csv">Download CSV File</a>'
        st.markdown(href, unsafe_allow_html=True)
    except Exception as e:
        st.error(f"Error converting DataFrame to CSV: {e}")

def compare_data(report, compare_report):
    merged_report = report.merge(
        compare_report,
        on=["page", "query"],
        suffixes=("_current", "_compare")
    )
    merged_report["clicks_diff"] = merged_report["clicks_current"] - merged_report["clicks_compare"]
    merged_report["impressions_diff"] = merged_report["impressions_current"] - merged_report["impressions_compare"]
    return merged_report

def show_dimensions_selector(search_type):
    available_dimensions = update_dimensions(search_type)
    return st.multiselect(
        "Select Dimensions:",
        available_dimensions,
        default=st.session_state.selected_dimensions,
        key="dimensions_selector"
    )

def show_fetch_data_button(
    webproperty, search_type, start_date, end_date,
    selected_dimensions, filter_keywords, filter_keywords_not, filter_url
):
    if st.button("Fetch Data"):
        progress = st.progress(0)
        # If comparing time periods
        if st.session_state.compare:
            compare_start_date = st.session_state.compare_start_date
            compare_end_date = st.session_state.compare_end_date
            compare_report = fetch_compare_data(
                webproperty, search_type,
                compare_start_date, compare_end_date,
                selected_dimensions, st.session_state.selected_device
            )
            if not compare_report.empty:
                st.write("### Comparison data fetched successfully!")

                # Now fetch the main data in chunked, sequential manner
                report = fetch_gsc_data_in_chunks(
                    webproperty,
                    search_type,
                    start_date,
                    end_date,
                    selected_dimensions,
                    device_type=st.session_state.selected_device,
                    filter_keywords=filter_keywords,
                    filter_keywords_not=filter_keywords_not,
                    filter_url=filter_url
                )
                merged_report = compare_data(report, compare_report)
                progress.progress(0.8)
                show_dataframe(merged_report)
                download_csv_link(merged_report)
            else:
                st.write("No comparison data found for the selected parameters.")
            progress.progress(1.0)
        else:
            # Single date range, chunked
            df = fetch_gsc_data_in_chunks(
                webproperty,
                search_type,
                start_date,
                end_date,
                selected_dimensions,
                device_type=st.session_state.selected_device,
                filter_keywords=filter_keywords,
                filter_keywords_not=filter_keywords_not,
                filter_url=filter_url
            )
            if not df.empty:
                st.write(f"### Data fetched successfully! Rows: {len(df)}")
                show_dataframe(df)
                download_csv_link(df)
            else:
                st.write("No data found for the selected parameters.")
        progress.progress(1.0)

###############################################################################
# 6) Main entry point
###############################################################################

def main():
    setup_streamlit()
    client_config = load_config()

    # 1) Prepare OAuth
    flow, auth_url = google_auth(client_config)
    st.session_state.auth_flow = flow
    st.session_state.auth_url = auth_url

    # 2) Gather query params with older Streamlit method
    params = st.experimental_get_query_params()
    st.write("**Debug:** st.experimental_get_query_params =>", params)

    # 3) Reassemble code if truncated
    auth_code = reassemble_auth_code(params)
    st.write("**Debug:** reassembled auth_code =>", auth_code)

    # 4) Exchange token if we have code & no existing credentials
    if auth_code and not st.session_state.get("credentials"):
        try:
            st.write("**Debug:** Attempting to fetch token with code:", auth_code)
            st.session_state.auth_flow.fetch_token(code=auth_code)
            st.session_state.credentials = st.session_state.auth_flow.credentials
            # Clear code from URL
            st.experimental_set_query_params()
            st.write("**Debug:** Token fetched successfully.")
        except Exception as e:
            st.error(f"Error fetching token: {e}")

    # 5) If still no credentials, prompt sign-in
    if not st.session_state.get("credentials"):
        show_google_sign_in(st.session_state.auth_url)
        return

    # 6) We have credentials; proceed with the app
    init_session_state()
    account = auth_search_console(client_config, st.session_state.credentials)
    properties = list_gsc_properties(st.session_state.credentials)

    if properties:
        webproperty = show_property_selector(properties, account)
        search_type = show_search_type_selector()
        date_range_selection = show_date_range_selector()

        if date_range_selection == "Custom Range":
            show_custom_date_inputs()
            start_date = st.session_state.custom_start_date
            end_date = st.session_state.custom_end_date
        else:
            start_date, end_date = calc_date_range(date_range_selection)

        selected_dimensions = show_dimensions_selector(search_type)

        # Example optional device selector:
        # st.session_state.selected_device = st.selectbox(
        #     "Select Device:",
        #     ["All Devices", "desktop", "mobile", "tablet"]
        # )

        show_comparison_option()
        show_filter_options()
        show_fetch_data_button(
            webproperty, search_type, start_date, end_date,
            selected_dimensions, st.session_state.filter_keywords,
            st.session_state.filter_keywords_not, st.session_state.filter_url
        )

if __name__ == "__main__":
    main()
