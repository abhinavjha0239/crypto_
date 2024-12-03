import gspread  
from oauth2client.service_account import ServiceAccountCredentials  
import requests  
import time  
import statistics  
import os  
import json  
import logging  
from datetime import datetime, timedelta, timezone  

# Configure logging  
logging.basicConfig(  
    level=logging.INFO,   
    format='%(asctime)s - %(levelname)s: %(message)s',  
    handlers=[  
        logging.StreamHandler(),  
        logging.FileHandler('crypto_tracker.log')  
    ]  
)  
logger = logging.getLogger(__name__)  

def validate_service_account_json():  
    """  
    Comprehensive validation of service account JSON credentials  
    """  
    try:  
        # Multiple potential sources for credentials  
        credentials_sources = [  
            os.environ.get('SERVICE_ACCOUNT_JSON'),  
            os.environ.get('GOOGLE_CREDENTIALS'),  
            os.getenv('SERVICE_ACCOUNT_JSON')  
        ]  

        # Detailed environment variable logging  
        logger.info("Scanning environment for service account credentials")  
        
        for source in credentials_sources:  
            if source:  
                try:  
                    # Thorough JSON parsing and validation  
                    service_account = json.loads(source.strip())  
                    
                    # Comprehensive key validation  
                    required_keys = [  
                        'type',   
                        'project_id',   
                        'private_key',   
                        'client_email',  
                        'client_id'  
                    ]  
                    
                    # Check for missing keys  
                    missing_keys = [  
                        key for key in required_keys   
                        if key not in service_account  
                    ]  
                    
                    if missing_keys:  
                        logger.warning(f"Missing keys: {missing_keys}")  
                        continue  
                    
                    # Additional type and structure checks  
                    if service_account.get('type') != 'service_account':  
                        logger.error("Invalid service account type")  
                        continue  
                    
                    logger.info("✅ Service Account JSON validated successfully")  
                    return service_account  
                
                except json.JSONDecodeError as json_err:  
                    logger.error(f"JSON Parsing Error: {json_err}")  
                except Exception as parse_err:  
                    logger.error(f"Credential Parsing Error: {parse_err}")  
        
        # If no valid credentials found  
        raise ValueError("No valid service account credentials found")  
    
    except Exception as e:  
        logger.critical(f"Critical validation error: {e}")  
        raise  

def setup_google_sheets_client():  
    """  
    Set up authenticated Google Sheets client with robust error handling  
    """  
    try:  
        # Get validated service account credentials  
        service_account_dict = validate_service_account_json()  
        
        # Define comprehensive scope  
        scope = [  
            'https://spreadsheets.google.com/feeds',   
            'https://www.googleapis.com/auth/drive',  
            'https://www.googleapis.com/auth/spreadsheets'  
        ]  
        
        # Authenticate and create client  
        creds = ServiceAccountCredentials.from_json_keyfile_dict(  
            service_account_dict,   
            scope  
        )  
        client = gspread.authorize(creds)  
        
        logger.info("Google Sheets client authenticated successfully")  
        return client  
    
    except Exception as e:  
        logger.critical(f"Google Sheets client setup failed: {e}")  
        raise  

def fetch_and_update_crypto_data(client):  
    """  
    Fetch cryptocurrency data and update Google Sheets with error handling  
    """  
    # CoinGecko API endpoint for top 50 cryptocurrencies  
    url = 'https://api.coingecko.com/api/v3/coins/markets?vs_currency=usd&order=market_cap_desc&per_page=50&page=1&sparkline=false'  
    
    # Set Indian Standard Time  
    tz = timezone(timedelta(hours=5, minutes=30))  
    
    try:  
        # Fetch cryptocurrency data with timeout  
        response = requests.get(url, timeout=10)  
        response.raise_for_status()  
        data = response.json()  
        
        # Open specific Google Sheet  
        sheet_url = 'https://docs.google.com/spreadsheets/d/100qOfV1LbRyW8pruJrRUJ4HIFUn3MY_Qgz8I_YburTc/edit?gid=0#gid=0'  
        sheet = client.open_by_url(sheet_url).sheet1  
        
        # Prepare rows with comprehensive data  
        rows = []  
        for coin in data:  
            row = [  
                coin['name'],  
                coin['symbol'].upper(),  
                f"${coin['current_price']:,.2f}",  
                f"${coin['market_cap']:,}",  
                f"${coin['total_volume']:,}",  
                f"{coin['price_change_percentage_24h']:.2f}%",  
                datetime.now(tz).strftime('%Y-%m-%d %H:%M:%S')  
            ]  
            rows.append(row)  
        
        # Update sheet with new data  
        sheet.clear()  
        sheet.update('A1:G51', rows)  
        
        # Advanced data analysis  
        top_5_by_market_cap = sorted(data, key=lambda x: x['market_cap'], reverse=True)[:5]  
        top_5_data = [  
            [coin['name'], coin['symbol'].upper(), f"${coin['current_price']:,.2f}"]   
            for coin in top_5_by_market_cap  
        ]  
        
        # Statistical calculations  
        prices = [coin['current_price'] for coin in data]  
        average_price = statistics.mean(prices)  
        price_changes = [coin['price_change_percentage_24h'] for coin in data]  
        highest_change = max(price_changes)  
        lowest_change = min(price_changes)  

        # Advanced reporting section  
        analysis_data = [  
            ["Analysis Metrics"],  
            [f"Average Price: ${average_price:,.2f}"],  
            [f"Highest 24h Change: {highest_change:.2f}%"],  
            [f"Lowest 24h Change: {lowest_change:.2f}%"],  
            [""],  
            ["Top 5 Cryptocurrencies by Market Cap:"]  
        ]  
        analysis_data.extend(top_5_data)  

        # Update analysis section of the sheet  
        sheet.update('I1:L20', analysis_data)  

        logger.info(f"Cryptocurrency data updated successfully at {datetime.now(tz)}")  

    except requests.RequestException as req_err:  
        logger.error(f"API Request Error: {req_err}")  
        # Optional: Implement retry mechanism  
        time.sleep(60)  # Wait before retrying  
    
    except gspread.exceptions.APIError as sheets_err:  
        logger.error(f"Google Sheets API Error: {sheets_err}")  
        # Additional error handling for sheets  
    
    except Exception as e:  
        logger.critical(f"Unexpected error in data fetching: {e}")  
        # Optional: Send alert or notification  

def monitor_and_retry(max_retries=3):  
    """  
    Robust monitoring with retry mechanism  
    """  
    retries = 0  
    while retries < max_retries:  
        try:  
            # Setup Google Sheets client  
            client = setup_google_sheets_client()  

            # Continuous data fetching  
            while True:  
                fetch_and_update_crypto_data(client)  
                # Adaptive sleep with jitter to prevent API rate limits  
                time.sleep(300 + random.uniform(-30, 30))  

        except Exception as e:  
            retries += 1  
            logger.warning(f"Attempt {retries} failed: {e}")  
            
            # Exponential backoff  
            wait_time = min(2 ** retries, 600)  # Max wait of 10 minutes  
            logger.info(f"Retrying in {wait_time} seconds...")  
            time.sleep(wait_time)  
    
    logger.critical("Maximum retries exceeded. Exiting.")  
    sys.exit(1)  

def health_check():  
    """  
    Periodic system health check  
    """  
    try:  
        # Check internet connectivity  
        requests.get('https://www.google.com', timeout=5)  
        
        # Check service account validity  
        validate_service_account_json()  
        
        logger.info("✅ System health check passed")  
        return True  
    
    except requests.ConnectionError:  
        logger.error("❌ No internet connection")  
    except Exception as e:  
        logger.error(f"❌ Health check failed: {e}")  
    
    return False  

def main():  
    """  
    Main execution orchestrator  
    """  
    # Pre-flight checks  
    if not health_check():  
        logger.critical("Pre-flight health check failed")  
        sys.exit(1)  
    
    # Set up graceful shutdown  
    try:  
        monitor_and_retry()  
    except KeyboardInterrupt:  
        logger.info("Process manually stopped")  
    except Exception as e:  
        logger.critical(f"Unhandled exception: {e}")  
        # Optional: Send error notification  
    finally:  
        logger.info("Crypto Tracker shutting down")  

# Enhanced error tracking and execution  
if __name__ == "__main__":  
    # Add missing imports  
    import sys  
    import random  

    # Ensure proper exception handling  
    try:  
        main()  
    except Exception as e:  
        logger.critical(f"Critical failure: {e}")  
        sys.exit(1)