import gspread  
from oauth2client.service_account import ServiceAccountCredentials  
import requests  
import time  
import statistics  
import os  
import json  
from datetime import datetime, timedelta, timezone  

# Get service account JSON from environment variable  
service_account_json = os.environ.get('SERVICE_ACCOUNT_JSON')  
service_account_dict = json.loads(service_account_json)  

# Google Sheets API setup  
scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']  
creds = ServiceAccountCredentials.from_json_keyfile_dict(service_account_dict, scope)  
client = gspread.authorize(creds)  

# Replace with your actual Google Sheets URL  
sheet = client.open_by_url('https://docs.google.com/spreadsheets/d/100qOfV1LbRyW8pruJrRUJ4HIFUn3MY_Qgz8I_YburTc/edit?gid=0#gid=0').sheet1  

# CoinGecko API endpoint  
url = 'https://api.coingecko.com/api/v3/coins/markets?vs_currency=usd&order=market_cap_desc&per_page=50&page=1&sparkline=false'  

# Set the Indian time zone  
tz = timezone(timedelta(hours=5, minutes=30))  # UTC+5:30 (India Standard Time)  

def fetch_and_update_crypto_data():  
    try:  
        # Fetch the top 50 cryptocurrencies  
        response = requests.get(url)  
        data = response.json()  

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

    except Exception as e:  
        print(f"An error occurred: {e}")  

# Main execution  
def main():  
    while True:  
        fetch_and_update_crypto_data()  
        # Wait for 5 minutes before updating the data again  
        time.sleep(300)  

if __name__ == "__main__":  
    main()