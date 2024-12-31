# main.py

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
import concurrent.futures
import time

IS_LOCAL = False

# We'll force the script to only use these two dimensions:
FORCED_DIMENSIONS = ["page", "query"]

DATE_RANGE_OPTIONS = [
    "Last 7 Days",
    "Last 30 Days",
    "Last 3 Months",
    "Last 6 Months",
    "Last 12 Months",
    "Last 16 Months",
    "Custom Range"
]

MAX_ROWS = 250_000  # We use .limit(250000) in queries
DF_PREVIEW_ROWS = 100

###############################################################################
# 1) Handle code truncation with older Streamlit using st.experimental_get_query_params()
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
# 2) Streamlit setup
###############################################################################

def setup_streamlit():
    st.set_page_config(page_title="Google Search Console API Connector", layout="wide")
    st.title("Google Search Console API Connector")
    st.subheader("Export Up To 250,000 Keywords Seamlessly — Parallel + Filtered")
    st.markdown("""
    **Requirements / Features**:
    1. Only pulling "page" + "query" dimensions.
    2. Excludes any row with 0 clicks.
    3. URL or subfolder filter is applied at the query level (faster).
    4. Parallel chunk fetching for speed.
    """)

def init_session_state():
    if "selected_property" not in st.session_state:
        st.session_state.selected_property = None
    if "selected_date_range" not in st.session_state:
        st.session_state.selected_date_range = "Last 7 Days"
    if "start_date" not in st.session_state:
        st.session_state.start_date = datetime.date.today() - datetime.timedelta(days=7)
    if "end_date" not in st.session_state:
        st.session_state.end_date = datetime.date.today()

    if "selected_device" not in st.session_state:
        st.session_state.selected_device = "All Devices"

    # We'll skip dimension selection from the UI since we only want [page, query].
    # But if you want them to be able to choose, you can restore dimension selectors.

    if "custom_start_date" not in st.session_state:
        st.session_state.custom_start_date = datetime.date.today() - datetime.timedelta(days=7)
    if "custom_end_date" not in st.session_state:
        st.session_state.custom_end_date = datetime.date.today()
    if "filter_url" not in st.session_state:
        st.session_state.filter_url = ""
    if "filter_keywords" not in st.session_state:
        st.session_state.filter_keywords = ""
    if "filter_keywords_not" not in st.session_state:
        st.session_state.filter_keywords_not = ""
    if "compare" not in st.session_state:
        st.session_state.compare = False
    if "compare_start_date" not in st.session_state:
        st.session_state.compare_start_date = datetime.date.today() - datetime.timedelta(days=14)
    if "compare_end_date" not in st.session_state:
        st.session_state.compare_end_date = datetime.date.today() - datetime.timedelta(days=7)

###############################################################################
# 3) OAuth flow
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
                # Must EXACTLY match your GCP "Authorized redirect URI"
            ),
        }
    }
    return client_config

def init_oauth_flow(client_config):
    scopes = ["https://www.googleapis.com/auth/webmasters.readonly"]
    redirect_uri = client_config["web"]["redirect_uris"][0]
    flow = Flow.from_client_config(client_config, scopes=scopes, redirect_uri=redirect_uri)
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
# 4) Parallel chunk fetching
###############################################################################

def _fetch_chunk_threaded(
    client_config,
    credentials,
    property_uri,
    search_type,
    device_type,
    filter_url,
    chunk_start,
    chunk_end
):
    """
    Each thread re-initializes the searchconsole client for concurrency safety.
    We only use dimensions = ["page","query"].

    We apply the URL filter at the query level using .filter("page","contains", filter_url)
    to reduce data from the start.

    .limit(250000) is used. If you consistently hit 250k, you may need smaller date slices.
    """
    token = {
        "token": credentials.token,
        "refresh_token": credentials.refresh_token,
        "token_uri": credentials.token_uri,
        "client_id": credentials.client_id,
        "client_secret": credentials.client_secret,
        "scopes": credentials.scopes,
        "id_token": getattr(credentials, "id_token", None),
    }
    account = searchconsole.authenticate(client_config=client_config, credentials=token)
    webproperty = account[property_uri]

    st.write(f"[PARALLEL] Fetching chunk {chunk_start} -> {chunk_end} (Thread init)")

    # Build the query
    query = webproperty.query.range(chunk_start, chunk_end).search_type(search_type)
    query = query.dimension(*FORCED_DIMENSIONS)

    if filter_url:
        # Apply URL subfolder filter directly in the query
        query = query.filter("page", "contains", filter_url)

    if device_type and device_type != "All Devices":
        query = query.filter("device", "equals", device_type.lower())

    # Retrieve up to 250k rows
    df_chunk = query.limit(250000).get().to_dataframe()
    df_chunk.reset_index(drop=True, inplace=True)

    st.write(f"[PARALLEL] Got {len(df_chunk)} rows for {chunk_start} -> {chunk_end}")
    return df_chunk

def fetch_gsc_data_parallel(
    client_config,
    credentials,
    property_uri,
    search_type,
    start_date,
    end_date,
    device_type=None,
    filter_url=None,
    filter_keywords=None,
    filter_keywords_not=None
):
    """
    Splits the date range into ~30-day chunks, fetches each chunk in parallel threads,
    then concatenates results. Also filters out zero-click rows.
    """
    # 1) Build chunk list
    chunk_size_days = 30  # Adjust if still hitting 250k limit
    chunks = []
    current_start = start_date

    while current_start <= end_date:
        chunk_end = current_start + datetime.timedelta(days=chunk_size_days - 1)
        if chunk_end > end_date:
            chunk_end = end_date
        chunks.append((current_start, chunk_end))
        current_start = chunk_end + datetime.timedelta(days=1)

    st.write(f"**Info:** Using parallel fetch with chunk_size={chunk_size_days} days. Found {len(chunks)} chunks.")

    results = []
    # 2) Submit to ThreadPool
    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
        future_map = {
            executor.submit(
                _fetch_chunk_threaded,
                client_config,
                credentials,
                property_uri,
                search_type,
                device_type,
                filter_url,
                c[0],
                c[1]
            ): (c[0], c[1])
            for c in chunks
        }

        for future in concurrent.futures.as_completed(future_map):
            chunk_dates = future_map[future]
            try:
                df_chunk = future.result()
                results.append(df_chunk)
            except Exception as e:
                st.write(f"[ERROR] Chunk {chunk_dates} failed: {e}")

    # 3) Combine
    if results:
        df_all = pd.concat(results, ignore_index=True)
        # Remove duplicates (if overlap)
        df_all.drop_duplicates(
            subset=FORCED_DIMENSIONS + ["clicks","impressions","ctr","position"],
            inplace=True
        )
    else:
        df_all = pd.DataFrame()

    # 4) Filter out rows with 0 clicks
    if not df_all.empty:
        df_all = df_all[df_all["clicks"] > 0]

    # 5) Keyword filters if needed
    if not df_all.empty:
        if filter_keywords:
            keywords = [kw.strip() for kw in filter_keywords.split(",")]
            df_all = df_all[df_all["query"].str.contains("|".join(keywords), case=False, na=False)]

        if filter_keywords_not:
            for kw_not in filter_keywords_not.split(","):
                kw_not = kw_not.strip()
                df_all = df_all[~df_all["query"].str.contains(kw_not, case=False, na=False)]

    return df_all

def fetch_compare_data_single(
    client_config,
    credentials,
    property_uri,
    search_type,
    compare_start_date,
    compare_end_date,
    device_type=None,
    filter_url=None
):
    """
    Single-call compare data. If you need chunked parallel for compare, replicate
    the same approach above. For simplicity, we keep it single query here.
    """
    account = searchconsole.authenticate(
        client_config=client_config,
        credentials={
            "token": credentials.token,
            "refresh_token": credentials.refresh_token,
            "token_uri": credentials.token_uri,
            "client_id": credentials.client_id,
            "client_secret": credentials.client_secret,
            "scopes": credentials.scopes,
            "id_token": getattr(credentials, "id_token", None),
        }
    )
    webproperty = account[property_uri]

    st.write("Fetching comparison data (single query).")
    progress = st.progress(0.5)

    query = webproperty.query.range(compare_start_date, compare_end_date).search_type(search_type)
    query = query.dimension(*FORCED_DIMENSIONS)

    if filter_url:
        query = query.filter("page", "contains", filter_url)

    if device_type and device_type != "All Devices":
        query = query.filter("device", "equals", device_type.lower())

    try:
        df = query.limit(250000).get().to_dataframe()
        df.reset_index(drop=True, inplace=True)
        df = df[df["clicks"] > 0]  # remove zero-click rows in compare
        st.write("Comparison data fetched.")
        progress.progress(1.0)
        return df
    except Exception as e:
        st.error(f"Comparison fetch error: {e}")
        progress.progress(1.0)
        return pd.DataFrame()

###############################################################################
# 5) UI + main app flow
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
    return account[selected_property], selected_property

def show_date_range_selector():
    return st.selectbox(
        "Select Date Range:",
        DATE_RANGE_OPTIONS,
        index=DATE_RANGE_OPTIONS.index(st.session_state.selected_date_range),
        key="date_range_selector"
    )

def show_custom_date_inputs():
    st.session_state.custom_start_date = st.date_input(
        "Start Date",
        st.session_state.custom_start_date
    )
    st.session_state.custom_end_date = st.date_input(
        "End Date",
        st.session_state.custom_end_date
    )

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
    st.session_state.filter_url = st.text_input("URL or Subfolder Filter (applied in the query)")

    st.session_state.filter_keywords = st.text_input("Keyword Filter (contains, separate multiple with commas)")
    st.session_state.filter_keywords_not = st.text_input("Keyword Filter (does not contain, separate multiple with commas)")

def compare_data(report, compare_report):
    merged_report = report.merge(
        compare_report,
        on=["page", "query"],
        suffixes=("_current", "_compare")
    )
    merged_report["clicks_diff"] = merged_report["clicks_current"] - merged_report["clicks_compare"]
    merged_report["impressions_diff"] = merged_report["impressions_current"] - merged_report["impressions_compare"]
    return merged_report

def show_dataframe(report):
    with st.expander("Preview the First 100 Rows"):
        st.dataframe(report.head(100))

def download_csv_link(report):
    try:
        report.reset_index(drop=True, inplace=True)
        csv_str = report.to_csv(index=False, encoding="utf-8-sig")
        b64_csv = base64.b64encode(csv_str.encode()).decode()
        href = f'<a href="data:file/csv;base64,{b64_csv}" download="search_console_data.csv">Download CSV File</a>'
        st.markdown(href, unsafe_allow_html=True)
    except Exception as e:
        st.error(f"Error converting DataFrame to CSV: {e}")

def show_fetch_data_button(
    client_config,
    credentials,
    property_uri,
    search_type,
    start_date,
    end_date,
    device_type,
    filter_url,
    filter_keywords,
    filter_keywords_not
):
    if st.button("Fetch Data"):
        progress = st.progress(0)
        if st.session_state.compare:
            # 1) Fetch compare data (single shot for example)
            compare_start_date = st.session_state.compare_start_date
            compare_end_date = st.session_state.compare_end_date
            compare_report = fetch_compare_data_single(
                client_config,
                credentials,
                property_uri,
                search_type,
                compare_start_date,
                compare_end_date,
                device_type=device_type,
                filter_url=filter_url
            )

            if not compare_report.empty:
                st.write("### Comparison data fetched successfully!")
                # 2) Now fetch main data in parallel chunks
                report = fetch_gsc_data_parallel(
                    client_config,
                    credentials,
                    property_uri,
                    search_type,
                    start_date,
                    end_date,
                    device_type=device_type,
                    filter_url=filter_url,
                    filter_keywords=filter_keywords,
                    filter_keywords_not=filter_keywords_not
                )
                merged_report = compare_data(report, compare_report)
                progress.progress(0.8)

                show_dataframe(merged_report)
                download_csv_link(merged_report)
            else:
                st.write("No comparison data found for the selected parameters.")

            progress.progress(1.0)
        else:
            # Single date range with parallel chunk fetch
            df = fetch_gsc_data_parallel(
                client_config,
                credentials,
                property_uri,
                search_type,
                start_date,
                end_date,
                device_type=device_type,
                filter_url=filter_url,
                filter_keywords=filter_keywords,
                filter_keywords_not=filter_keywords_not
            )
            if not df.empty:
                st.write(f"### Data fetched successfully! Rows: {len(df)}")
                show_dataframe(df)
                download_csv_link(df)
            else:
                st.write("No data found for the selected parameters.")
        progress.progress(1.0)

###############################################################################
# 6) Main
###############################################################################

def main():
    setup_streamlit()
    client_config = load_config()
    flow, auth_url = google_auth(client_config)
    st.session_state.auth_flow = flow
    st.session_state.auth_url = auth_url

    params = st.experimental_get_query_params()
    st.write("**Debug:** st.experimental_get_query_params =>", params)

    auth_code = reassemble_auth_code(params)
    st.write("**Debug:** reassembled auth_code =>", auth_code)

    if auth_code and not st.session_state.get("credentials"):
        try:
            st.write("**Debug:** Attempting to fetch token with code:", auth_code)
            st.session_state.auth_flow.fetch_token(code=auth_code)
            st.session_state.credentials = st.session_state.auth_flow.credentials
            st.experimental_set_query_params()  # Clear code from URL
            st.write("**Debug:** Token fetched successfully.")
        except Exception as e:
            st.error(f"Error fetching token: {e}")

    if not st.session_state.get("credentials"):
        show_google_sign_in(st.session_state.auth_url)
        return

    init_session_state()

    # 1) Convert the credentials for usage
    credentials = st.session_state.credentials

    # 2) Build the search console account object once, for listing properties, etc.
    account = auth_search_console(client_config, credentials)
    properties = list_gsc_properties(credentials)

    if properties:
        webproperty, property_uri = show_property_selector(properties, account)
        date_range_selection = show_date_range_selector()

        if date_range_selection == "Custom Range":
            show_custom_date_inputs()
            start_date = st.session_state.custom_start_date
            end_date = st.session_state.custom_end_date
        else:
            start_date, end_date = calc_date_range(date_range_selection)

        search_type = st.selectbox(
            "Select Search Type:",
            ["web", "image", "video", "news", "discover", "googleNews"],
            index=0
        )

        # If you want a device selector:
        st.session_state.selected_device = st.selectbox(
            "Select Device:",
            ["All Devices", "desktop", "mobile", "tablet"]
        )

        show_comparison_option()
        show_filter_options()

        show_fetch_data_button(
            client_config,
            credentials,
            property_uri,  # pass property_uri instead of webproperty
            search_type,
            start_date,
            end_date,
            st.session_state.selected_device,
            st.session_state.filter_url,
            st.session_state.filter_keywords,
            st.session_state.filter_keywords_not
        )

if __name__ == "__main__":
    main()
