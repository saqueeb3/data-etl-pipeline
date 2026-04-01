import os
from dotenv import load_dotenv

load_dotenv()

CSV_FILE = os.getenv('CSV_FILE', r'D:\Data Engineering\data-etl-project\data\raw\data.csv')
SERVER = os.getenv('SQL_SERVER', r'localhost\SQLEXPRESS')
DATABASE = os.getenv('SQL_DATABASE', 'data_engineering')