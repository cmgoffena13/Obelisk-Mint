import os
from dotenv import load_dotenv

dotenv_path = os.path.join(os.path.dirname(__file__), '.env')
load_dotenv(dotenv_path)

sql_username = os.environ.get("SQL_USERNAME")
sql_password = os.environ.get("SQL_PASSWORD")
mint_username = os.environ.get("MINT_USERNAME")
mint_password = os.environ.get("MINT_PASSWORD")
