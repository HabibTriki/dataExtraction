import os
import threading
import time
import requests
import logging
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(os.path.join("logs", "data_collection.log")),
        logging.StreamHandler()
    ]
)

TOKEN_URL = os.getenv("TOKEN_URL")
CLIENT_ID = os.getenv("CLIENT_ID")
CLIENT_SECRET = os.getenv("CLIENT_SECRET")
REFRESH_GRACE = 60
_refresh_lock = threading.Lock()

class AuthClient:
    def __init__(self):
        self.token_data = {"access_token": None, "expires_at": 0}

    def get_token(self):
        with _refresh_lock:
            current_time = time.time()
            if self.token_data["access_token"] and self.token_data["expires_at"] > current_time + REFRESH_GRACE:
                logging.info("Using cached token")
                return self.token_data["access_token"]

            logging.info("Requesting new token")
            try:
                response = requests.post(TOKEN_URL, data={
                    "client_id": CLIENT_ID,
                    "client_secret": CLIENT_SECRET,
                    "grant_type": "client_credentials",
                    "scope": "openid"
                })
                response.raise_for_status()
                result = response.json()
                self.token_data["access_token"] = result["access_token"]
                self.token_data["expires_at"] = current_time + result.get("expires_in", 3600)
                logging.info(f"New token obtained, expires in {result['expires_in']} seconds")
                return self.token_data["access_token"]
            except Exception as e:
                logging.error(f"Error during token retrieval: {e}")
                raise

auth_client = AuthClient()
def get_token():
    return auth_client.get_token()

if __name__ == "__main__":
    print("Token:", auth_client.get_token())
