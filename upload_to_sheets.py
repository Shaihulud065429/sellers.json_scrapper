from google_sheets_uploader import GoogleSheetsUploader

# Your Google Sheet ID
SPREADSHEET_ID = '16rptcM-d1tgxFid2NeS3BQjjOuxODNK7ZIng_DUDGag'

def main():
    try:
        # Initialize the uploader
        uploader = GoogleSheetsUploader(SPREADSHEET_ID)
        
        # Upload all data
        uploader.upload_all_data()
        print("Successfully uploaded all data to Google Sheets!")
        
    except Exception as e:
        print(f"Error during upload: {str(e)}")

if __name__ == "__main__":
    main() 