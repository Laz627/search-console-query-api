import os
import pandas as pd
import streamlit as st
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from google.auth.transport.requests import Request
from googleapiclient.errors import HttpError

# Define constants
TOKEN_FILE = 'token.json'
SCOPES = ['https://www.googleapis.com/auth/webmasters.readonly']

def get_service(credentials_file):
    creds = None
    # Check if the token file exists
    if os.path.exists(TOKEN_FILE):
        creds = Credentials.from_authorized_user_file(TOKEN_FILE, SCOPES)
    # If there are no valid credentials available, prompt the user to log in
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                credentials_file, SCOPES,
                redirect_uri='https://YOUR_PROJECT_NAME.streamlit.app'
            )
            auth_url, _ = flow.authorization_url(prompt='consent')

            st.write("Please go to this URL and authorize the app:")
            st.write(auth_url)

            code = st.text_input("Enter the authorization code:")

            if code:
                flow.fetch_token(code=code)
                creds = flow.credentials
                # Save the credentials for the next run
                with open(TOKEN_FILE, 'w') as token:
                    token.write(creds.to_json())

    service = build('webmasters', 'v3', credentials=creds)
    return service

def fetch_search_analytics(service, site_url, start_date, end_date, row_limit=25000):
    data = []
    start_row = 0

    while True:
        request = {
            'startDate': start_date,
            'endDate': end_date,
            'dimensions': ['query', 'page'],
            'rowLimit': row_limit,
            'startRow': start_row
        }
        
        try:
            response = service.searchanalytics().query(siteUrl=site_url, body=request).execute()
        except HttpError as err:
            st.error(f"An error occurred: {err}")
            return pd.DataFrame()  # Return an empty DataFrame in case of error
        
        rows = response.get('rows', [])
        if not rows:
            break
        
        for row in rows:
            data.append({
                'query': row['keys'][0],
                'page': row['keys'][1],
                'clicks': row['clicks'],
                'impressions': row['impressions'],
                'ctr': row['ctr'],
                'position': row['position']
            })
        
        start_row += row_limit
    
    return pd.DataFrame(data)

def process_data(credentials_file, site_url, date_ranges):
    service = get_service(credentials_file)
    
    data_frames = []
    total_ranges = len(date_ranges)
    for idx, date_range in enumerate(date_ranges, 1):
        st.write(f"Processing data for date range: {date_range['label']} ({idx}/{total_ranges})")
        df = fetch_search_analytics(service, site_url, date_range['startDate'], date_range['endDate'])
        if df.empty:
            st.error("Failed to fetch data.")
            return pd.DataFrame()  # Return an empty DataFrame if fetching fails
        df['date_range'] = date_range['label']
        data_frames.append(df)
        st.write(f"Completed processing for date range: {date_range['label']}")

    st.write("Combining data frames...")
    combined_df = pd.concat(data_frames)
    
    st.write("Pivoting data for comparison...")
    comparison_df = combined_df.pivot_table(
        index=['query', 'page'],
        columns='date_range',
        values=['clicks', 'impressions', 'ctr', 'position'],
        aggfunc='sum'
    ).reset_index()

    # Flatten the MultiIndex columns
    comparison_df.columns = ['_'.join(col).strip() if type(col) is tuple else col for col in comparison_df.columns.values]

    st.write("Filling empty cells with 0...")
    comparison_df = comparison_df.fillna(0)

    st.write("Calculating differences between the specified date ranges for clicks and impressions...")
    if 'clicks_Range 1' in comparison_df.columns and 'clicks_Range 2' in comparison_df.columns:
        comparison_df['clicks_diff'] = comparison_df['clicks_Range 2'] - comparison_df['clicks_Range 1']
        comparison_df['impressions_diff'] = comparison_df['impressions_Range 2'] - comparison_df['impressions_Range 1']

    st.write("Sorting data by clicks for the latest date range from largest to smallest...")
    comparison_df = comparison_df.sort_values(by='clicks_Range 2', ascending=False)
    
    st.write("Tagging keywords as branded or nonbranded...")
    comparison_df['keyword_type'] = comparison_df['query_'].apply(lambda x: 'branded' if 'us bank' in x.lower() else 'nonbranded')
    
    return comparison_df

# Streamlit app layout
st.title('Google Search Console Data Analysis')

uploaded_file = st.file_uploader("Upload your OAuth 2.0 credentials JSON file", type="json")

site_url = st.text_input("Enter your root domain (e.g., https://www.example.com)")

date_range_1_start = st.date_input("Start date for the first date range")
date_range_1_end = st.date_input("End date for the first date range")
date_range_2_start = st.date_input("Start date for the second date range")
date_range_2_end = st.date_input("End date for the second date range")

if uploaded_file and site_url and date_range_1_start and date_range_1_end and date_range_2_start and date_range_2_end:
    date_ranges = [
        {'startDate': date_range_1_start.strftime('%Y-%m-%d'), 'endDate': date_range_1_end.strftime('%Y-%m-%d'), 'label': 'Range 1'},
        {'startDate': date_range_2_start.strftime('%Y-%m-%d'), 'endDate': date_range_2_end.strftime('%Y-%m-%d'), 'label': 'Range 2'}
    ]
    
    with open("uploaded_credentials.json", "wb") as f:
        f.write(uploaded_file.getbuffer())
        
    if st.button('Fetch and Process Data'):
        df = process_data("uploaded_credentials.json", site_url, date_ranges)
        st.write("Data fetched and processed successfully.")
        st.write("Here's a preview of the data:")
        st.write(df.head())
        
        csv = df.to_csv(index=False)
        st.download_button(
            label="Download data as CSV",
            data=csv,
            file_name='search_analytics_comparison.csv',
            mime='text/csv',
        )
