'''#Got tis dataset(data.csv) from kaggle website, it contains something like 540k data, it was like 48MB file.
.................................................................................................................
#E-Commerce ETL Pipeline
# This one orchestrates the entire ETL process for e-commerce transaction data.
# It reads data from a CSV file, cleans and transforms it, then loads it into SQL Server.
# The pipeline runs in three phases:
# 1. Extract - Read data from CSV 
# 2. Transform - Clean, validate, and enrich the data
# 3. Load - Insert into database tables
.................................................................................................................
#I built this to process online retail transaction data and create a normalized database 
# for analysis. The pipeline handles data quality issues like duplicates, missing values,and cancelled orders before loading into the database.
'''
import logging  #original script didn't had any logs, got this idea from a friend
import sys
import os
from datetime import datetime

# Importing the modules from the extract.py,transform.py and load.py
from extract import extract_data
from transform import transform_data
from load import DatabaseConnection

#i have no idea how it worked, this block of code was not done by me
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('etl_pipeline.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


# ============================================================================
# Configuration of the server and the dataset
# ============================================================================
CSV_FILE = r'D:\Data Engineering\data-etl-project\data\raw\data.csv'
SCHEMA_FILE = r'D:\Data Engineering\data-etl-project\sql\schema.sql'

# SQL Server connection details (Windows Authentication)
SERVER = 'SAQUEEB\SQLEXPRESS01'  #tried to make it 007, but though it'll be too long for my short turm memory
DATABASE = 'EcommerceETL'
USE_WINDOWS_AUTH = True  #i used the window authentication in my SQL server express 2025, yes i am poor.


# ============================================================================
# Main Pipeline
# ============================================================================
def run_etl_pipeline():
    """here this function will execute the whole pipeline system"""
    
    logger.info("=" * 70)
    logger.info("Starting E-Commerce ETL Pipeline")
    logger.info("=" * 70)
    
    start_time = datetime.now()
    
    #so, from here the extraction begins, lets call it phase 1
    logger.info("")
    logger.info("[PHASE 1] EXTRACT")
    logger.info("-" * 70)
    df = extract_data(CSV_FILE)
    
    if df is None: #i hope this one doesnt get executed
                    #guess what this one was executed
                    #Oh fixed it, i didn't gave the right path
        logger.error("Pipeline failed at extraction stage")
        return False
    
    logger.info("Extraction completed successfully")
    logger.info("")
    
    # Phase 2: Transform {no i am not writing the whole line like here transformation starts}
    #after writing it i realised i just wrote even longer lune
    logger.info("[PHASE 2] TRANSFORM")
    logger.info("-" * 70)
    df = transform_data(df)
    
    if df is None:
        logger.error("Pipeline failed at transformation stage")
        return False
    
    logger.info("Transformation completed successfully")
    logger.info("")
    
    # Phase 3: Load
    logger.info("[PHASE 3] LOAD")
    logger.info("-" * 70)
    
    '''Yeah so here, all this connect to the Database as the data is already extracted from the sataset and also transformed.
    '''
    db = DatabaseConnection(
        server=SERVER,
        database=DATABASE,
        use_windows_auth=USE_WINDOWS_AUTH
    )
    
    # Connectipn to database
    if not db.connect():
        logger.error("Pipeline failed to connect to SQL Server")
        return False
    '''this block runs the schemma and i hate this part cause i already know it will DEFINITELY give some error
     Guess what?? it did!!!!!
      FINALLLY, YES!!! '''
    try:
        with open(SCHEMA_FILE, 'r') as f:
            schema_script = f.read()
        
        if not db.execute_script(schema_script):
            logger.error("Pipeline failed to create schema")
            db.disconnect()
            return False
        
        logger.info("Schema created successfully")
    except FileNotFoundError:
        logger.error("Schema file not found: %s" % SCHEMA_FILE)
        db.disconnect()
        return False
    except Exception as e:
        logger.error("Failed to read schema file: %s" % str(e))
        db.disconnect()
        return False
    
    '''yes this one is the block that got added because as i tested it few times,
    the tables and DB was already there and was causing conflict between the pre existing values and the new values that were being loaded into the DB and it was super annoyingg'''
    logger.info("Clearing old data...")
    if not db.clear_tables():
        logger.error("Pipeline failed to clear old data")
        db.disconnect()
        return False
    
    # Loading sata to db
    if not db.load_products(df):
        logger.error("Failed to load products")
        db.disconnect()
        return False
    
    if not db.load_customers(df):
        logger.error("Failed to load customers")
        db.disconnect()
        return False
    
    if not db.load_transactions(df):
        logger.error("Failed to load transactions")
        db.disconnect()
        return False
    
    logger.info("Data loading completed successfully")
    logger.info("")

    db.disconnect()
    
    #again, thanks to my friend
    end_time = datetime.now()
    duration = end_time - start_time    
    logger.info("=" * 70)
    logger.info("ETL Pipeline completed successfully!")
    logger.info("Total duration: %s" % str(duration))
    logger.info("=" * 70)
    return True

if __name__ == "__main__":

    success = run_etl_pipeline()
    sys.exit(0 if success else 1)