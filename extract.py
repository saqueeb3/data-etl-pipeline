"""
Extraction module, basically this one will pill all the content from the dataset
thank god pandas library
"""

import pandas as pd
import logging

logger = logging.getLogger(__name__)


def extract_data(csv_path):

    """
   UTF-8 (most common) - handles any character worldwide
Latin-1 (iso-8859-1) - used mainly for European languages
ASCII - basic English characters only"""
#any guess what this is??NO? look at the end

    logger.info(f"Starting extraction from {csv_path}")
    
    try:
        df = pd.read_csv(csv_path, encoding='latin-1')
        logger.info(f"Successfully extracted {len(df)} rows and {len(df.columns)} columns")
        logger.info(f"Columns: {list(df.columns)}")
        return df
        
    except FileNotFoundError:
        logger.error(f"CSV file not found: {csv_path}")
        return None
    except Exception as e:
        logger.error(f"Failed to extract data: {e}")
        return None
    

    #yeah look here
''' as it turns out different dataset support different encoding, by default and the most common one is UFT-8
but ofcourse dataset i chose was not UFT-8, it was latin-1
do i know what UFT-8 is? NO!
do i know what latin-1 is? also NO!
do i care? ABSOLUTELY NO!!'''