import os
import pandas as pd
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
import pickle
from datetime import datetime
import logging

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('sheets_upload.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# If modifying these scopes, delete the file token.pickle.
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']

class GoogleSheetsUploader:
    def __init__(self, spreadsheet_id):
        self.spreadsheet_id = spreadsheet_id
        self.creds = None
        self.service = None
        self._authenticate()

    def _authenticate(self):
        """Authenticate with Google Sheets API."""
        if os.path.exists('token.pickle'):
            with open('token.pickle', 'rb') as token:
                self.creds = pickle.load(token)

        if not self.creds or not self.creds.valid:
            if self.creds and self.creds.expired and self.creds.refresh_token:
                self.creds.refresh(Request())
            else:
                flow = InstalledAppFlow.from_client_secrets_file(
                    'credentials.json', SCOPES)
                self.creds = flow.run_local_server(port=0)
            
            with open('token.pickle', 'wb') as token:
                pickle.dump(self.creds, token)

        self.service = build('sheets', 'v4', credentials=self.creds)

    def clean_dataframe(self, df):
        """Clean DataFrame by handling problematic values."""
        try:
            # Replace NaN with empty string
            df = df.fillna("")
            
            # Convert all values to strings and truncate if too long
            for col in df.columns:
                df[col] = df[col].astype(str).apply(lambda x: x[:49000] if len(x) > 49000 else x)
            
            return df
        except Exception as e:
            logger.error(f"Error cleaning DataFrame: {str(e)}")
            return df

    def upload_dataframe(self, df, sheet_name):
        """Upload a pandas DataFrame to a specific sheet."""
        if df.empty:
            logger.info(f"No data to upload for {sheet_name}")
            return

        try:
            # Clean the DataFrame
            df = self.clean_dataframe(df)
            
            # Convert DataFrame to list of lists
            values = [df.columns.tolist()]
            
            # Process rows one by one to handle errors
            for index, row in df.iterrows():
                try:
                    # Convert row to list and ensure all values are strings
                    row_values = [str(val) for val in row.values]
                    values.append(row_values)
                except Exception as e:
                    logger.warning(f"Skipping row {index} in {sheet_name} due to error: {str(e)}")
                    continue

            # Format range name without quotes
            range_name = f"{sheet_name}!A1"
            
            # First, ensure the sheet exists
            try:
                self.service.spreadsheets().values().get(
                    spreadsheetId=self.spreadsheet_id,
                    range=range_name
                ).execute()
            except Exception:
                # If sheet doesn't exist, create it
                body = {
                    'requests': [{
                        'addSheet': {
                            'properties': {
                                'title': sheet_name
                            }
                        }
                    }]
                }
                self.service.spreadsheets().batchUpdate(
                    spreadsheetId=self.spreadsheet_id,
                    body=body
                ).execute()

            # Clear existing content
            self.service.spreadsheets().values().clear(
                spreadsheetId=self.spreadsheet_id,
                range=range_name
            ).execute()

            # Update with new data
            body = {
                'values': values
            }
            
            result = self.service.spreadsheets().values().update(
                spreadsheetId=self.spreadsheet_id,
                range=range_name,
                valueInputOption='RAW',
                body=body
            ).execute()

            logger.info(f"Updated {result.get('updatedCells')} cells in {sheet_name}")
        except Exception as e:
            logger.error(f"Error updating sheet {sheet_name}: {str(e)}")
            raise

    def upload_all_data(self):
        """Upload all output files to Google Sheets."""
        try:
            # Upload sellers data
            if os.path.exists('output/sellers_data.csv'):
                try:
                    df_sellers = pd.read_csv('output/sellers_data.csv', low_memory=False)
                    self.upload_dataframe(df_sellers, 'Sellers Data')
                except Exception as e:
                    logger.error(f"Error processing sellers_data.csv: {str(e)}")

            # Upload direct media data
            if os.path.exists('output/direct_media.csv'):
                try:
                    # Try reading with semicolon separator first
                    try:
                        df_direct = pd.read_csv('output/direct_media.csv', sep=';', low_memory=False)
                    except:
                        # If that fails, try with comma separator
                        df_direct = pd.read_csv('output/direct_media.csv', low_memory=False)
                    self.upload_dataframe(df_direct, 'Direct Media')
                except Exception as e:
                    logger.error(f"Error processing direct_media.csv: {str(e)}")

            # Upload intermediaries data
            if os.path.exists('output/intermediaries.csv'):
                try:
                    df_inter = pd.read_csv('output/intermediaries.csv', low_memory=False)
                    self.upload_dataframe(df_inter, 'Intermediaries')
                except Exception as e:
                    logger.error(f"Error processing intermediaries.csv: {str(e)}")

            # Upload new domains report
            if os.path.exists('output/new_domains_report.csv'):
                try:
                    df_new = pd.read_csv('output/new_domains_report.csv', low_memory=False)
                    self.upload_dataframe(df_new, 'New Domains Report')
                except Exception as e:
                    logger.error(f"Error processing new_domains_report.csv: {str(e)}")

            # Add timestamp
            timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            self.upload_dataframe(
                pd.DataFrame({'Last Updated': [timestamp]}),
                'Last Updated'
            )
        except Exception as e:
            logger.error(f"Error in upload_all_data: {str(e)}")
            raise

    def append_dataframe(self, df, sheet_name):
        """Append a pandas DataFrame to a specific sheet, creating the sheet if it doesn't exist. Does not overwrite existing data."""
        if df.empty:
            logger.info(f"No data to append for {sheet_name}")
            return
        try:
            df = self.clean_dataframe(df)
            values = df.values.tolist()
            # Ensure the sheet exists
            range_name = f"{sheet_name}!A1"
            try:
                self.service.spreadsheets().values().get(
                    spreadsheetId=self.spreadsheet_id,
                    range=range_name
                ).execute()
            except Exception:
                # If sheet doesn't exist, create it
                body = {
                    'requests': [{
                        'addSheet': {
                            'properties': {
                                'title': sheet_name
                            }
                        }
                    }]
                }
                self.service.spreadsheets().batchUpdate(
                    spreadsheetId=self.spreadsheet_id,
                    body=body
                ).execute()
            # Append data (do not include headers)
            body = {
                'values': values
            }
            result = self.service.spreadsheets().values().append(
                spreadsheetId=self.spreadsheet_id,
                range=range_name,
                valueInputOption='RAW',
                insertDataOption='INSERT_ROWS',
                body=body
            ).execute()
            logger.info(f"Appended {result.get('updates', {}).get('updatedRows', 0)} rows to {sheet_name}")
        except Exception as e:
            logger.error(f"Error appending to sheet {sheet_name}: {str(e)}")
            raise

# Set the environment variable for your Google Sheet ID
os.environ['GOOGLE_SHEETS_ID'] = 'your-spreadsheet-id-here'