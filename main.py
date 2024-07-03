import streamlit as st
import pandas as pd
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import Flow
from googleapiclient.discovery import build
import os
import json

# Define constants
SCOPES = ['https://www.googleapis.com/auth/webmasters.readonly']
TOKEN_FILE = 'token.json'
CLIENT_SECRETS_FILE = 'client_secret.json'

# OAuth flow to get credentials
def get_credentials():
    creds = None
    if 'credentials' in st.session_state:
        creds = Credentials.from_authorized_user_info(st.session_state['credentials'], SCOPES)
    else:
        flow = Flow.from_client_secrets_file(CLIENT_SECRETS_FILE, SCOPES)
        flow.redirect_uri = 'https://your-app-name.streamlit.app'
        auth_url, _ = flow.authorization_url(prompt='consent')
        st.write(f"Please go to this URL and authorize the app: [Authorize]({auth_url})")

        code = st.text_input("Enter the authorization code here:")
        if st.button("Submit Authorization Code"):
            try:
                flow.fetch_token(code=code)
                creds = flow.credentials
                st.session_state['credentials'] = json.loads(creds.to_json())
            except Exception as e:
                st.error(f"An error occurred: {e}")
                st.stop()

    return creds

# Function to fetch data from Google Search Console
def fetch_data(service, site_url, start_date, end_date):
    request = {
        'startDate': start_date,
        'endDate': end_date,
        'dimensions': ['query'],
        'rowLimit': 25000
    }
    response = service.searchanalytics().query(siteUrl=site_url, body=request).execute()
    return response

# Main function
def main():
    st.title('Google Search Console Data Analysis')

    creds = get_credentials()

    if creds:
        service = build('webmasters', 'v3', credentials=creds)

        site_url = st.text_input("Enter your root domain (e.g., https://www.example.com)")

        date_range_1_start = st.date_input("Start date for the first date range")
        date_range_1_end = st.date_input("End date for the first date range")
        date_range_2_start = st.date_input("Start date for the second date range")
        date_range_2_end = st.date_input("End date for the second date range")

        if st.button('Fetch and Process Data'):
            data_1 = fetch_data(service, site_url, date_range_1_start.strftime('%Y-%m-%d'), date_range_1_end.strftime('%Y-%m-%d'))
            data_2 = fetch_data(service, site_url, date_range_2_start.strftime('%Y-%m-%d'), date_range_2_end.strftime('%Y-%m-%d'))

            df1 = pd.DataFrame(data_1['rows'])
            df2 = pd.DataFrame(data_2['rows'])

            st.write("Data fetched successfully.")
            st.write("Here's a preview of the data:")
            st.write(df1.head())
            st.write(df2.head())

if __name__ == "__main__":
    main()
