import streamlit as st
import pandas as pd
import json
import os
from google.oauth2 import service_account
from google.auth.transport.requests import Request
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build

# Define constants
SCOPES = ['https://www.googleapis.com/auth/webmasters.readonly']
TOKEN_FILE = 'token.json'

def get_service():
    creds = None
    if os.path.exists(TOKEN_FILE):
        creds = service_account.Credentials.from_authorized_user_file(TOKEN_FILE, SCOPES)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file('client_secret.json', SCOPES)
            auth_url, _ = flow.authorization_url(prompt='consent')

            st.write("Please go to this URL and authorize the app:")
            st.write(auth_url)

            code = st.text_input("Enter the authorization code here:")

            if st.button("Submit Authorization Code"):
                flow.fetch_token(code=code)
                creds = flow.credentials
                with open(TOKEN_FILE, 'w') as token:
                    token.write(creds.to_json())
    return build('webmasters', 'v3', credentials=creds)

def fetch_data(service, site_url, start_date, end_date):
    request = {
        'startDate': start_date,
        'endDate': end_date,
        'dimensions': ['query'],
        'rowLimit': 25000
    }
    response = service.searchanalytics().query(siteUrl=site_url, body=request).execute()
    return response

st.title('Google Search Console Data Analysis')

uploaded_file = st.file_uploader("Upload your OAuth 2.0 credentials JSON file", type="json")
site_url = st.text_input("Enter your root domain (e.g., https://www.example.com)")

date_range_1_start = st.date_input("Start date for the first date range")
date_range_1_end = st.date_input("End date for the first date range")
date_range_2_start = st.date_input("Start date for the second date range")
date_range_2_end = st.date_input("End date for the second date range")

if uploaded_file and site_url and date_range_1_start and date_range_1_end and date_range_2_start and date_range_2_end:
    with open("client_secret.json", "wb") as f:
        f.write(uploaded_file.getbuffer())
        
    if st.button('Fetch and Process Data'):
        service = get_service()
        data_1 = fetch_data(service, site_url, date_range_1_start.strftime('%Y-%m-%d'), date_range_1_end.strftime('%Y-%m-%d'))
        data_2 = fetch_data(service, site_url, date_range_2_start.strftime('%Y-%m-%d'), date_range_2_end.strftime('%Y-%m-%d'))
        
        # Process data and display results
        st.write("Data fetched successfully.")
        st.write("Here's a preview of the data:")
        st.write(pd.DataFrame(data_1['rows']).head())
        st.write(pd.DataFrame(data_2['rows']).head())

        # Further processing and comparison can be added here
