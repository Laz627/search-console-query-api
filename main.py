import datetime
import base64
import re
import urllib.parse
import streamlit as st
from google_auth_oauthlib.flow import Flow
from googleapiclient.discovery import build
import pandas as pd
import searchconsole
import concurrent.futures

IS_LOCAL = False

# Only "page" + "query" dimensions:
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

MAX_ROWS = 250_000  # Each chunk query is limited to 250k rows.

###############################################################################
# 1) This function handles older Streamlit versions that truncate auth code.
###############################################################################
def reassemble_auth_code(params):
    """
    If code was split around a slash, rejoin it.
    E.g. if st.experimental_get_query_params() shows {"code":["4"],"0AanRRr...":[""]}
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
# 2) Setup Streamlit (instructions, session state)
###############################################################################
def setup_streamlit():
    st.set_page_config(page_title="GSC Parallel Exporter", layout="wide")

    st.title("Google Search Console Parallel Exporter")
    st.markdown(
        """
        **Instructions**:
        1. Use the sidebar to **Sign in with Google**.
        2. Choose your GSC property & date range.
        3. Optionally enter a **subfolder** like `"/features-options/"`. 
           - If you type `"pella.com/features-options"`, we'll extract just `"/features-options"`.
        4. (Optional) Check 'Compare Time Periods' for a second date range.
        5. Click **"Fetch Data"** to retrieve chunked results in **parallel** (only page + query), excluding zero-click rows.
        6. Preview the first 100 rows, then **download** your CSV.
        """
    )

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
# 3) OAuth + Search Console
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
def _extract_subfolder(input_str: str) -> str:
    """
    Extract just the subfolder portion starting from the first slash.
    Examples:
      "pella.com/ideas"  -> "/ideas"
      "https://pella.com/features-options/" -> "/features-options/"
      "/features-options/" -> "/features-options/"
      "pella.com" -> (no slash => keep "pella.com")
    """
    if not input_str:
        return ""
    # Strip known prefixes like "http://", "https://"
    # We'll let urlparse handle them, then extract the path.
    parsed = urllib.parse.urlparse(input_str, scheme='', allow_fragments=False)
    if parsed.netloc:
        # If there's a domain, parsed.path is the subfolder
        if parsed.path:
            return parsed.path  # e.g. "/ideas"
        else:
            return parsed.netloc  # e.g. "pella.com" if no slash
    # If user typed "pella.com/ideas" w/o protocol => netloc won't parse
    # We'll manually find first slash:
    idx = input_str.find("/")
    if idx >= 0:
        return input_str[idx:]  # everything from slash forward
    return input_str  # no slash found => keep as is

def _fetch_chunk_threaded(
    client_config,
    credentials,
    property_uri,
    search_type,
    device_type,
    subfolder,
    chunk_start,
    chunk_end
):
    # Re-initialize searchconsole per thread for concurrency safety
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

    query = webproperty.query.range(chunk_start, chunk_end).search_type(search_type)
    query = query.dimension(*FORCED_DIMENSIONS)

    # Apply subfolder filter with .filter("page", "contains", subfolder)
    if subfolder:
        query = query.filter("page", "contains", subfolder)

    if device_type and device_type != "All Devices":
        query = query.filter("device", "equals", device_type.lower())

    df_chunk = query.limit(MAX_ROWS).get().to_dataframe()
    df_chunk.reset_index(drop=True, inplace=True)
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
    # Extract the subfolder from filter_url (removing domain/protocol)
    subfolder = _extract_subfolder(filter_url)

    # Build ~30-day chunks
    chunk_size_days = 30
    chunks = []
    current_start = start_date
    while current_start <= end_date:
        chunk_end = current_start + datetime.timedelta(days=chunk_size_days - 1)
        if chunk_end > end_date:
            chunk_end = end_date
        chunks.append((current_start, chunk_end))
        current_start = chunk_end + datetime.timedelta(days=1)

    results = []
    total_chunks = len(chunks)
    progress_bar = st.progress(0)
    progress_text = st.empty()

    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
        future_map = {
            executor.submit(
                _fetch_chunk_threaded,
                client_config,
                credentials,
                property_uri,
                search_type,
                device_type,
                subfolder,
                c[0],
                c[1]
            ): c
            for c in chunks
        }

        done_count = 0
        for future in concurrent.futures.as_completed(future_map):
            date_range = future_map[future]
            try:
                df_chunk = future.result()
                results.append(df_chunk)
            except Exception as e:
                st.error(f"[ERROR] Chunk {date_range} failed: {e}")
            done_count += 1
            progress_bar.progress(done_count / total_chunks)
            progress_text.write(f"Fetched {done_count} of {total_chunks} chunks.")

    # Combine
    if results:
        df_all = pd.concat(results, ignore_index=True)
        df_all.drop_duplicates(
            subset=FORCED_DIMENSIONS + ["clicks","impressions","ctr","position"],
            inplace=True
        )
    else:
        df_all = pd.DataFrame()

    # Exclude zero clicks
    if not df_all.empty:
        df_all = df_all[df_all["clicks"] > 0]

    # Keyword filters
    if not df_all.empty:
        if filter_keywords:
            keywords = [kw.strip() for kw in filter_keywords.split(",")]
            df_all = df_all[df_all["query"].str.contains("|".join(keywords), case=False, na=False)]

        if filter_keywords_not:
            for kw_not in filter_keywords_not.split(","):
                kw_not = kw_not.strip()
                df_all = df_all[~df_all["query"].str.contains(kw_not, case=False, na=False)]

    progress_bar.progress(1.0)
    progress_text.write("All chunks fetched and combined.")
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
    # Single query for the compare timeframe
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

    query = webproperty.query.range(compare_start_date, compare_end_date).search_type(search_type)
    query = query.dimension(*FORCED_DIMENSIONS)

    subfolder = _extract_subfolder(filter_url)
    if subfolder:
        query = query.filter("page", "contains", subfolder)

    if device_type and device_type != "All Devices":
        query = query.filter("device", "equals", device_type.lower())

    try:
        df = query.limit(MAX_ROWS).get().to_dataframe()
        df.reset_index(drop=True, inplace=True)
        # Remove zero-click
        df = df[df["clicks"] > 0]
        return df
    except Exception as e:
        st.error(f"Comparison fetch error: {e}")
        return pd.DataFrame()

###############################################################################
# 5) UI and final app flow
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
        "Start Date", st.session_state.custom_start_date
    )
    st.session_state.custom_end_date = st.date_input(
        "End Date", st.session_state.custom_end_date
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
    st.session_state.compare = st.checkbox("Compare Time Periods", value=st.session_state.compare)
    if st.session_state.compare:
        st.session_state.compare_start_date = st.date_input(
            "Comparison Start Date", st.session_state.compare_start_date
        )
        st.session_state.compare_end_date = st.date_input(
            "Comparison End Date", st.session_state.compare_end_date
        )

def show_filter_options():
    st.session_state.filter_url = st.text_input("Subfolder Filter (e.g. /features-options/)")
    st.session_state.filter_keywords = st.text_input("Keyword Filter (contains, comma-separated)")
    st.session_state.filter_keywords_not = st.text_input("Exclude Keywords (comma-separated)")

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
        if st.session_state.compare:
            compare_df = fetch_compare_data_single(
                client_config,
                credentials,
                property_uri,
                search_type,
                st.session_state.compare_start_date,
                st.session_state.compare_end_date,
                device_type=device_type,
                filter_url=filter_url
            )
            if not compare_df.empty:
                main_df = fetch_gsc_data_parallel(
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
                if not main_df.empty:
                    merged = compare_data(main_df, compare_df)
                    if not merged.empty:
                        st.success("Data fetched successfully with comparison!")
                        show_dataframe(merged)
                        download_csv_link(merged)
                    else:
                        st.warning("Comparison merge returned no overlapping data.")
                else:
                    st.warning("No data found for the main time period.")
            else:
                st.warning("No data found for the comparison time period.")
        else:
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
                st.success(f"Data fetched successfully! Rows: {len(df)}")
                show_dataframe(df)
                download_csv_link(df)
            else:
                st.warning("No data found for the selected parameters.")

###############################################################################
# 6) Main Entry
###############################################################################
def main():
    setup_streamlit()
    client_config = load_config()
    flow, auth_url = google_auth(client_config)
    st.session_state.auth_flow = flow
    st.session_state.auth_url = auth_url

    # For older Streamlit, use st.experimental_get_query_params
    params = st.experimental_get_query_params()
    auth_code = reassemble_auth_code(params)

    # Try to fetch token if we have an auth_code and no credentials
    if auth_code and not st.session_state.get("credentials"):
        try:
            st.session_state.auth_flow.fetch_token(code=auth_code)
            st.session_state.credentials = st.session_state.auth_flow.credentials
            st.experimental_set_query_params()
        except Exception as e:
            st.error(f"Error fetching token: {e}")

    if not st.session_state.get("credentials"):
        show_google_sign_in(st.session_state.auth_url)
        return

    init_session_state()
    credentials = st.session_state.credentials
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
        st.session_state.selected_device = st.selectbox(
            "Select Device:",
            ["All Devices", "desktop", "mobile", "tablet"]
        )

        show_comparison_option()
        show_filter_options()

        show_fetch_data_button(
            client_config,
            credentials,
            property_uri,
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
