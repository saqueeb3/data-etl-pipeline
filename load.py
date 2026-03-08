"""
Loading Module, the final crusade!
Handles database connection and loading transformed data into the SQL Server
"""

import pyodbc 
import logging

logger = logging.getLogger(__name__)

'''i am tired i will just ask AI to generate the comments for this one, Goodnight'''
class DatabaseConnection:
    """
    Manages SQL Server database connections and all data loading operations.
    Handles connecting, disconnecting, and inserting data into three tables:
    Products, Customers, and Transactions.
    """
    #still need a job so doing hybrid work AI + myself.
    
    def __init__(self, server, database, use_windows_auth=True, username=None, password=None):
        """
        Initialize database connection parameters
        
        Args:
            server (str): SQL Server instance name
            database (str): Database name
            use_windows_auth (bool): Use Windows Authentication if True, SQL auth if False
            username (str): SQL Server username (only needed if use_windows_auth=False)
            password (str): SQL Server password (only needed if use_windows_auth=False)
        """
        self.server = server
        self.database = database
        self.use_windows_auth = use_windows_auth
        self.username = username
        self.password = password
        self.connection = None
    
    def connect(self):
        """Establish connection to SQL Server"""
        try:
            if self.use_windows_auth:
                # Windows Authentication (uses current Windows user)
                connection_string = f'Driver={{ODBC Driver 17 for SQL Server}};Server={self.server};Database={self.database};Trusted_Connection=yes;'
            else:
                # SQL Server Authentication (username/password)
                connection_string = f'Driver={{ODBC Driver 17 for SQL Server}};Server={self.server};Database={self.database};UID={self.username};PWD={self.password}'
            
            self.connection = pyodbc.connect(connection_string)
            logger.info(f"Successfully connected to {self.database} on {self.server}")
            return True
        except Exception as e:
            logger.error(f"Failed to connect to SQL Server: {e}")
            return False
    
    def disconnect(self):
        """Close database connection"""
        if self.connection:
            self.connection.close()
            logger.info("Database connection closed")
    
    def execute_script(self, script):
        """
        Execute SQL script (for schema creation)
        
        Args:
            script (str): SQL script to execute
            
        Returns:
            bool: True if successful, False otherwise
        """
        if not self.connection:
            logger.error("No database connection")
            return False
        
        try:
            cursor = self.connection.cursor()
            # Split script by GO statements (SQL Server batch separator)
            batches = script.split('GO')
            
            for batch in batches:
                batch = batch.strip()
                if batch:
                    cursor.execute(batch)
            
            self.connection.commit()
            logger.info("SQL script executed successfully")
            return True
            
        except Exception as e:
            logger.error(f"Failed to execute SQL script: {e}")
            self.connection.rollback()
            return False
    
    def clear_tables(self):
        """
        Clear all tables in the correct order (respecting foreign key constraints)
        Delete from child tables first, then parent tables
        
        Returns:
            bool: True if successful, False otherwise
        """
        logger.info("Clearing old data from tables...")
        
        try:
            cursor = self.connection.cursor()
            
            # Delete in this order: Transactions (child) -> Customers -> Products
            cursor.execute("DELETE FROM dbo.Transactions")
            logger.info("Cleared Transactions table")
            
            cursor.execute("DELETE FROM dbo.Customers")
            logger.info("Cleared Customers table")
            
            cursor.execute("DELETE FROM dbo.Products")
            logger.info("Cleared Products table")
            
            self.connection.commit()
            logger.info("All tables cleared successfully")
            return True
            
        except Exception as e:
            logger.error(f"Failed to clear tables: {e}")
            self.connection.rollback()
            return False
    
    def load_products(self, df):
        """
        Load unique products into Products table
        Handle duplicates by keeping only the first occurrence
        Args:
            df (pd.DataFrame): Transformed data   
        Returns:
            bool: True if successful, False otherwise
        """
        logger.info("Loading products...")
        
        try:
            cursor = self.connection.cursor()
            
            # Get unique products - keep first occurrence only
            products = df[['StockCode', 'Description']].drop_duplicates(subset=['StockCode'], keep='first')
            logger.info(f"Found {len(products)} unique products to load")
            
            for idx, row in products.iterrows():
                try:
                    cursor.execute(
                        "INSERT INTO dbo.Products (StockCode, Description) VALUES (?, ?)",
                        row['StockCode'], row['Description']
                    )
                except Exception as e:
                    # Log but continue if one product fails
                    logger.warning(f"Warning loading product {row['StockCode']}: {e}")
                    continue
            
            self.connection.commit()
            logger.info(f"Loaded {len(products)} unique products")
            return True
            
        except Exception as e:
            logger.error(f"Failed to load products: {e}")
            self.connection.rollback()
            return False
    
    def load_customers(self, df):
        """
        Load unique customers with aggregated data into Customers table
        Args: {learned this Args from a fellow coder, dont know what it means thoug}
            df (pd.DataFrame): Transformed data
        Returns:
            bool: True if successful, False otherwise
        """
        logger.info("Loading customers...")
        
        try:
            cursor = self.connection.cursor()
            
            # Aggregate customer data
            customer_data = df.groupby('CustomerID').agg({
                'Country': 'first',
                'InvoiceDate': ['min', 'max', 'count']
            }).reset_index()
            customer_data.columns = ['CustomerID', 'Country', 'FirstPurchaseDate', 'LastPurchaseDate', 'TotalPurchases']
            
            for _, row in customer_data.iterrows():
                try:
                    cursor.execute(
                        """INSERT INTO dbo.Customers 
                           (CustomerID, Country, FirstPurchaseDate, LastPurchaseDate, TotalPurchases) 
                           VALUES (?, ?, ?, ?, ?)""",
                        int(row['CustomerID']), row['Country'], row['FirstPurchaseDate'], 
                        row['LastPurchaseDate'], int(row['TotalPurchases'])
                    )
                except Exception as e:
                    logger.warning(f"Warning loading customer {row['CustomerID']}: {e}")
                    continue
            
            self.connection.commit()
            logger.info(f"Loaded {len(customer_data)} unique customers")
            return True
            
        except Exception as e:
            logger.error(f"Failed to load customers: {e}")
            self.connection.rollback()
            return False
    
    def load_transactions(self, df):
        """
        Load all transactions into Transactions table
        given:
            df (pd.DataFrame): Transformed data
            
        should returns:
            bool: True if successful, False for anything else
        """
        logger.info("Loading transactions...")
        
        try:
            cursor = self.connection.cursor()
            
            for i, row in df.iterrows():
                try:
                    cursor.execute(
                        """INSERT INTO dbo.Transactions 
                           (InvoiceNo, StockCode, Quantity, UnitPrice, TotalSalesAmount, InvoiceDate, 
                            InvoiceYear, InvoiceMonth, InvoiceDay, CustomerID, Country)
                           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                        row['InvoiceNo'], row['StockCode'], int(row['Quantity']), 
                        float(row['UnitPrice']), float(row['TotalSalesAmount']), 
                        row['InvoiceDate'], int(row['InvoiceYear']), int(row['InvoiceMonth']), 
                        int(row['InvoiceDay']), int(row['CustomerID']), row['Country']
                    )
                except Exception as e:
                    logger.warning(f"Warning loading transaction {i}: {e}")
                    continue
                
                # Batch commit every 10k rows for performance
                if (i + 1) % 10000 == 0:
                    self.connection.commit()
                    logger.info(f"Committed {i + 1} transaction rows...")
            
            self.connection.commit()
            logger.info(f"Loaded {len(df)} transactions")
            return True
            
        except Exception as e:
            logger.error(f"Failed to load transactions: {e}")
            self.connection.rollback()
            return False