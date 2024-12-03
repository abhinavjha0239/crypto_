import gspread  
from oauth2client.service_account import ServiceAccountCredentials  
import requests  
import time  
import statistics  
import os  
import json  
from datetime import datetime, timedelta, timezone  
import logging  

# Configure logging  
logging.basicConfig(  
    level=logging.INFO,  
    format='%(asctime)s - %(levelname)s: %(message)s',  
    handlers=[  
        logging.StreamHandler(),  
        logging.FileHandler('crypto_tracker.log', encoding='utf-8')  
    ]  
)  

class CryptoTracker:  
    def __init__(self):  
        self.logger = logging.getLogger(self.__class__.__name__)  
        self.tz = timezone(timedelta(hours=5, minutes=30))  # Indian Standard Time  

    def get_service_account_credentials(self):  
        """  
        Retrieve and validate service account credentials from environment variables  
        """  
        try:  
            service_account_dict = {  
                'type': os.environ.get('TYPE', ''),  
                'project_id': os.environ.get('PROJECT_ID', ''),  
                'private_key_id': os.environ.get('PRIVATE_KEY_ID', ''),  
                'private_key': os.environ.get('PRIVATE_KEY', '').replace('\\n', '\n'),  
                'client_email': os.environ.get('CLIENT_EMAIL', ''),  
                'client_id': os.environ.get('CLIENT_ID', ''),  
                'auth_uri': os.environ.get('AUTH_URI', ''),  
                'token_uri': os.environ.get('TOKEN_URI', ''),  
                'auth_provider_x509_cert_url': os.environ.get('AUTH_PROVIDER_X509_CERT_URL', ''),  
                'client_x509_cert_url': os.environ.get('CLIENT_X509_CERT_URL', ''),  
                'universe_domain': os.environ.get('UNIVERSE_DOMAIN', '')  
            }  

            # Validate required keys  
            required_keys = ['type', 'project_id', 'private_key', 'client_email']  
            if all(service_account_dict.get(key) for key in required_keys):  
                self.logger.info("✅ Service account credentials successfully loaded")  
                return service_account_dict  
            
            raise ValueError("Missing required service account credentials")  

        except Exception as e:  
            self.logger.error(f"Credentials Error: {e}")  
            raise  

    def setup_google_sheets_client(self):  
        """  
        Setup Google Sheets client using service account credentials  
        """  
        try:  
            service_account_dict = self.get_service_account_credentials()  
            
            # Google Sheets API scope  
            scope = [  
                'https://spreadsheets.google.com/feeds',   
                'https://www.googleapis.com/auth/drive'  
            ]  
            
            # Authorize credentials  
            creds = ServiceAccountCredentials.from_json_keyfile_dict(  
                service_account_dict,   
                scope  
            )  
            client = gspread.authorize(creds)  

            return client  

        except Exception as e:  
            self.logger.error(f"Google Sheets Client Setup Failed: {e}")  
            raise  

    def fetch_cryptocurrency_data(self):  
        """  
        Fetch top 50 cryptocurrencies from CoinGecko API  
        """  
        url = 'https://api.coingecko.com/api/v3/coins/markets?vs_currency=usd&order=market_cap_desc&per_page=50&page=1&sparkline=false'  
        
        try:  
            response = requests.get(url, timeout=10)  
            response.raise_for_status()  
            return response.json()  
        
        except requests.RequestException as e:  
            self.logger.error(f"API Fetch Error: {e}")  
            raise  

    def analyze_crypto_data(self, data):  
        """  
        Perform data analysis on cryptocurrency data  
        """  
        try:  
            # Top 5 by market cap  
            top_5_by_market_cap = sorted(  
                data,   
                key=lambda x: x['market_cap'],   
                reverse=True  
            )[:5]  

            top_5_data = [  
                [coin['name'], coin['symbol'], coin['current_price']]   
                for coin in top_5_by_market_cap  
            ]  

            prices = [coin['current_price'] for coin in data]  
            average_price = statistics.mean(prices)  

            price_changes = [coin['price_change_percentage_24h'] for coin in data]  
            highest_change = max(price_changes)  
            lowest_change = min(price_changes)  

            return {  
                'top_5': top_5_data,  
                'average_price': average_price,  
                'highest_change': highest_change,  
                'lowest_change': lowest_change  
            }  

        except Exception as e:  
            self.logger.error(f"Data Analysis Error: {e}")  
            raise  

    def update_google_sheet(self, client, data, analyzed_data):  
        """  
        Update Google Sheet with cryptocurrency data  
        """  
        try:  
            # Replace with your actual Google Sheets URL  
            sheet_url = os.environ.get(  
                'GOOGLE_SHEET_URL',   
                'https://docs.google.com/spreadsheets/d/100qOfV1LbRyW8pruJrRUJ4HIFUn3MY_Qgz8I_YburTc/edit'  
            )  
            sheet = client.open_by_url(sheet_url).sheet1  

            # Prepare rows with cryptocurrency data  
            rows = []  
            for coin in data:  
                row = [  
                    coin['name'],  
                    coin['symbol'],  
                    coin['current_price'],  
                    coin['market_cap'],  
                    coin['total_volume'],  
                    coin['price_change_percentage_24h'],  
                    datetime.now(self.tz).strftime('%Y-%m-%d %H:%M:%S')  
                ]  
                rows.append(row)  

            # Clear and update sheet  
            sheet.clear()  
            sheet.update('A1:G51', rows)  

            # Update analysis section  
            sheet.update_cell(52, 1, "Top 5 Cryptocurrencies by Market Cap:")  
            sheet.update('A53:C57', analyzed_data['top_5'])  
            sheet.update_cell(59, 1, f"Average price: ${analyzed_data['average_price']:.2f}")  
            sheet.update_cell(60, 1, f"Highest 24h change: {analyzed_data['highest_change']:.2f}%")  
            sheet.update_cell(61, 1, f"Lowest 24h change: {analyzed_data['lowest_change']:.2f}%")  

            self.logger.info("Google Sheet updated successfully")  

        except Exception as e:  
            self.logger.error(f"Google Sheet Update Error: {e}")  
            raise  

    def run(self):  
        """  
        Main execution method  
        """  
        try:  
            # Setup Google Sheets client  
            client = self.setup_google_sheets_client()  

            while True:  
                try:  
                    # Fetch cryptocurrency data  
                    crypto_data = self.fetch_cryptocurrency_data()  

                    # Analyze data  
                    analyzed_data = self.analyze_crypto_data(crypto_data)  

                    # Update Google Sheet  
                    self.update_google_sheet(client, crypto_data, analyzed_data)  

                    # Wait for 5 minutes  
                    self.logger.info("Waiting 5 minutes before next update...")  
                    time.sleep(300)  

                except Exception as inner_error:  
                    self.logger.error(f"Inner loop error: {inner_error}")  
                    time.sleep(60)  # Wait a minute before retrying  

        except Exception as e:  
            self.logger.critical(f"Critical error: {e}")  

def main():  
    tracker = CryptoTracker()  
    tracker.run()  

if __name__ == "__main__":  
    main()