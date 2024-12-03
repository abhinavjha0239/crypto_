import gspread  
from oauth2client.service_account import ServiceAccountCredentials  
import requests  
import time  
import statistics  
import os  
import json  
from datetime import datetime, timedelta, timezone  

def get_service_account_credentials():  
    # Multiple methods to retrieve the service account JSON  
    credentials_sources = [  
        os.environ.get('SERVICE_ACCOUNT_JSON'),  
        os.environ.get('GOOGLE_CREDENTIALS'),  
        os.getenv('SERVICE_ACCOUNT_JSON')  
    ]  

    # Debug: Print environment variables  
    print("Environment Variables:")  
    for key, value in os.environ.items():  
        if 'SERVICE_ACCOUNT' in key or 'CREDENTIALS' in key:  
            print(f"{key}: {'*' * 10 if value else 'EMPTY'}")  

    # Try each source  
    for source in credentials_sources:  
        if source:  
            try:  
                # Ensure it's a string and strip any whitespace  
                service_account_json = source.strip()  
                
                # Attempt to parse the JSON  
                service_account_dict = json.loads(service_account_json)  
                
                # Validate the dictionary has required keys  
                required_keys = ['type', 'project_id', 'private_key', 'client_email']  
                if all(key in service_account_dict for key in required_keys):  
                    print("✅ Service account credentials successfully loaded")  
                    return service_account_dict  
                
                print(f"❌ Invalid service account JSON: Missing required keys")  
            
            except json.JSONDecodeError as e:  
                print(f"❌ JSON Decode Error: {e}")  
                print("Ensure the SERVICE_ACCOUNT_JSON is a valid JSON string")  
            
            except Exception as e:  
                print(f"❌ Unexpected error: {e}")  

    # If no valid credentials found  
    raise ValueError("""  
    NO VALID SERVICE ACCOUNT CREDENTIALS FOUND!   
    Please set SERVICE_ACCOUNT_JSON environment variable in Railway.  
    Ensure:  
    1. You've copied the ENTIRE service account JSON  
    2. No extra spaces or formatting issues  
    3. The JSON is a SERVICE ACCOUNT key (not OAuth client)  
    """)  

def setup_google_sheets_client():  
    # Get service account credentials  
    service_account_dict = get_service_account_credentials()  

    # Google Sheets API setup  
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']  
    creds = ServiceAccountCredentials.from_json_keyfile_dict(service_account_dict, scope)  
    client = gspread.authorize(creds)  

    return client  

def fetch_and_update_crypto_data(client):  
    # CoinGecko API endpoint  
    url = 'https://api.coingecko.com/api/v3/coins/markets?vs_currency=usd&order=market_cap_desc&per_page=50&page=1&sparkline=false'  

    # Set the Indian time zone  
    tz = timezone(timedelta(hours=5, minutes=30))  # UTC+5:30 (India Standard Time)  

    try:  
        # Fetch the top 50 cryptocurrencies  
        response = requests.get(url, timeout=10)  
        response.raise_for_status()  # Raise an exception for bad status codes  
        data = response.json()  

        # Replace with your actual Google Sheets URL  
        sheet = client.open_by_url('https://docs.google.com/spreadsheets/d/100qOfV1LbRyW8pruJrRUJ4HIFUn3MY_Qgz8I_YburTc/edit?gid=0#gid=0').sheet1  

        # Extract the required data fields  
        rows = []  
        for coin in data:  
            row = [  
                coin['name'],  
                coin['symbol'],  
                coin['current_price'],  
                coin['market_cap'],  
                coin['total_volume'],  
                coin['price_change_percentage_24h'],  
                datetime.now(tz).strftime('%Y-%m-%d %H:%M:%S')  
            ]  
            rows.append(row)  

        # Update the Google Sheets document  
        sheet.clear()  
        sheet.update('A1:G51', rows)  

        # Data analysis  
        top_5_by_market_cap = sorted(data, key=lambda x: x['market_cap'], reverse=True)[:5]  
        top_5_data = [[coin['name'], coin['symbol'], coin['current_price']] for coin in top_5_by_market_cap]  

        prices = [coin['current_price'] for coin in data]  
        average_price = statistics.mean(prices)  

        price_changes = [coin['price_change_percentage_24h'] for coin in data]  
        highest_change = max(price_changes)  
        lowest_change = min(price_changes)  

        # Update the analyzed data in the Google Sheets document  
        sheet.update_cell(52, 1, "Top 5 Cryptocurrencies by Market Cap:")  
        sheet.update('A53:C57', top_5_data)  

        sheet.update_cell(59, 1, f"Average price of the top 50 cryptocurrencies: ${average_price:.2f}")  
        sheet.update_cell(60, 1, f"Highest 24-hour price change: {highest_change:.2f}%")  
        sheet.update_cell(61, 1, f"Lowest 24-hour price change: {lowest_change:.2f}%")  

        print(f"Data updated successfully at {datetime.now(tz)}")  

    except requests.RequestException as e:  
        print(f"Error fetching cryptocurrency data: {e}")  
    except Exception as e:  
        print(f"An unexpected error occurred: {e}")  

def main():  
    try:  
        # Setup Google Sheets client  
        client = setup_google_sheets_client()  

        # Main loop  
        while True:  
            fetch_and_update_crypto_data(client)  
            # Wait for 5 minutes before updating the data again  
            time.sleep(300)  

    except Exception as e:  
        print(f"Critical error in main function: {e}")  
        # Optional: Add more robust error handling or logging  
        import traceback  
        traceback.print_exc()  

if __name__ == "__main__":  
    main()