"""
Transformation Module, the second step of the almighty ETL
Here i cleaned the data, validated it ans all 
"""

import pandas as pd
import logging

logger = logging.getLogger(__name__)


def transform_data(df):
    """
    Transformation and cleaning of the raw data
    given this:
        df (pd.DataFrame): Raw data from extraction
    it should returns:
        pd.DataFrame: Transformed data, or None if transformation fails
    """
    logger.info("Starting transformation phase")
    
    try:
        if df is None or len(df) == 0:
            logger.error("Input dataframe is empty or None")
            return None
        
        # Step 1: Loging the starting state
        logger.info(f"Initial row count: {len(df)}")
        logger.info(f"Missing values:\n{df.isnull().sum()}")
        
        # Step 2: Removing the null values, but not just normal empty space, these 3 attribute are critical
        df = df.dropna(subset=['InvoiceNo', 'StockCode', 'CustomerID'])
        logger.info(f"Rows after removing nulls in critical columns: {len(df)}")
        
        # Step 3:removing the dups, no redundancy in my kingdom
        initial_rows = len(df)
        df = df.drop_duplicates()
        logger.info(f"Removed {initial_rows - len(df)} duplicate rows")
        
        # Step 4: balidating the date in a sensible format
        df['InvoiceDate'] = pd.to_datetime(df['InvoiceDate'], format='%m/%d/%Y %H:%M')
        logger.info(f"Date range: {df['InvoiceDate'].min()} to {df['InvoiceDate'].max()}")
        
        # Step 5:do we care about cancelled orders? NO, so just remove them , i mean why not
        before_cancel = len(df)
        df = df[~df['InvoiceNo'].str.startswith('C', na=False)]
        logger.info(f"Removed {before_cancel - len(df)} cancelled orders")
        
        # Step 6:can you order -1 items? or 0 items? EXACTLY
        before_filter = len(df)
        df = df[(df['Quantity'] > 0) & (df['UnitPrice'] > 0)]
        logger.info(f"Removed {before_filter - len(df)} rows with invalid quantities/prices")
        
        # Step 7:because money matters, for the data analytics work obviously, what did you thought? 
        df['TotalSalesAmount'] = df['Quantity'] * df['UnitPrice']
        logger.info("Created TotalSalesAmount column")
        
        # Step 8:no more spacebar
        string_cols = ['InvoiceNo', 'StockCode', 'Description', 'Country']
        for col in string_cols:
            df[col] = df[col].str.strip()
        logger.info("Removed whitespace from string columns")
        
        # Step 9:once my teacher said standardization, it never left me
        df['CustomerID'] = df['CustomerID'].astype('Int64')  # nullable int
        logger.info("Converted CustomerID to integer type")
        
        # Step 10: Extracting the  date components for better querying so i don't cry in SQL
        df['InvoiceYear'] = df['InvoiceDate'].dt.year
        df['InvoiceMonth'] = df['InvoiceDate'].dt.month
        df['InvoiceDay'] = df['InvoiceDate'].dt.day
        logger.info("Extracted date components (Year, Month, Day)")
        
        logger.info(f"Final transformed row count: {len(df)}")
        logger.info("Transformation complete")
        
        return df
        
    except Exception as e:
        logger.error(f"Failed during transformation: {e}")
        return None
