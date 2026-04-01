"""
Star Schema Loading Module
Handles loading data into dim_product, dim_customer, dim_date, and fact_sales
Also manages the watermark for incremental loading
"""

import pyodbc
import logging

logger = logging.getLogger(__name__)


class StarSchemaLoader:

    def __init__(self, server, database):
        self.server = server
        self.database = database
        self.connection = None

    def connect(self):
        try:
            conn_str = (
                f"DRIVER={{ODBC Driver 17 for SQL Server}};"
                f"SERVER={self.server};"
                f"DATABASE={self.database};"
                f"Trusted_Connection=yes;"
            )
            self.connection = pyodbc.connect(conn_str)
            logger.info(f"Connected to {self.database} on {self.server}")
            return True
        except Exception as e:
            logger.error(f"Connection failed: {e}")
            return False

    def disconnect(self):
        if self.connection:
            self.connection.close()
            logger.info("Connection closed")

    # ================================================================
    # WATERMARK - get last loaded date
    # ================================================================
    def get_watermark(self):
        try:
            cursor = self.connection.cursor()
            cursor.execute("SELECT LastLoadedDate FROM dbo.watermark WHERE TableName = 'fact_sales'")
            row = cursor.fetchone()
            return row[0] if row else None
        except Exception as e:
            logger.error(f"Failed to get watermark: {e}")
            return None

    def update_watermark(self, new_date):
        try:
            cursor = self.connection.cursor()
            cursor.execute(
                "UPDATE dbo.watermark SET LastLoadedDate = ? WHERE TableName = 'fact_sales'",
                new_date
            )
            self.connection.commit()
            logger.info(f"Watermark updated to {new_date}")
            return True
        except Exception as e:
            logger.error(f"Failed to update watermark: {e}")
            return False

    # ================================================================
    # LOAD dim_date
    # ================================================================
    def load_dim_date(self, dim_date_df):
        logger.info("Loading dim_date...")
        try:
            cursor = self.connection.cursor()
            loaded = 0
            for _, row in dim_date_df.iterrows():
                try:
                    cursor.execute("""
                        IF NOT EXISTS (SELECT 1 FROM dbo.dim_date WHERE DateKey = ?)
                        INSERT INTO dbo.dim_date
                            (DateKey, FullDate, Year, Month, MonthName, Quarter, Day, DayOfWeek, IsWeekend)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    row['DateKey'],
                    row['DateKey'], row['FullDate'], row['Year'], row['Month'],
                    row['MonthName'], row['Quarter'], row['Day'],
                    row['DayOfWeek'], row['IsWeekend'])
                    loaded += 1
                except Exception as e:
                    logger.warning(f"Skipping date {row['DateKey']}: {e}")
                    continue
            self.connection.commit()
            logger.info(f"Loaded {loaded} rows into dim_date")
            return True
        except Exception as e:
            logger.error(f"Failed to load dim_date: {e}")
            self.connection.rollback()
            return False

    # ================================================================
    # LOAD dim_product
    # ================================================================
    def load_dim_product(self, df):
        logger.info("Loading dim_product...")
        try:
            cursor = self.connection.cursor()
            products = df[['StockCode', 'Description']].drop_duplicates(subset=['StockCode'], keep='first')
            loaded = 0
            for _, row in products.iterrows():
                try:
                    cursor.execute("""
                        IF NOT EXISTS (SELECT 1 FROM dbo.dim_product WHERE StockCode = ?)
                        INSERT INTO dbo.dim_product (StockCode, Description)
                        VALUES (?, ?)
                    """, row['StockCode'], row['StockCode'], row['Description'])
                    loaded += 1
                except Exception as e:
                    logger.warning(f"Skipping product {row['StockCode']}: {e}")
                    continue
            self.connection.commit()
            logger.info(f"Loaded {loaded} rows into dim_product")
            return True
        except Exception as e:
            logger.error(f"Failed to load dim_product: {e}")
            self.connection.rollback()
            return False

    # ================================================================
    # LOAD dim_customer
    # ================================================================
    def load_dim_customer(self, df):
        logger.info("Loading dim_customer...")
        try:
            cursor = self.connection.cursor()
            customer_data = df.groupby('CustomerID').agg(
                Country=('Country', 'first'),
                FirstPurchaseDate=('InvoiceDate', 'min'),
                LastPurchaseDate=('InvoiceDate', 'max')
            ).reset_index()
            loaded = 0
            for _, row in customer_data.iterrows():
                try:
                    cursor.execute("""
                        IF NOT EXISTS (SELECT 1 FROM dbo.dim_customer WHERE CustomerID = ?)
                        INSERT INTO dbo.dim_customer
                            (CustomerID, Country, FirstPurchaseDate, LastPurchaseDate)
                        VALUES (?, ?, ?, ?)
                    """,
                    int(row['CustomerID']),
                    int(row['CustomerID']), row['Country'],
                    row['FirstPurchaseDate'], row['LastPurchaseDate'])
                    loaded += 1
                except Exception as e:
                    logger.warning(f"Skipping customer {row['CustomerID']}: {e}")
                    continue
            self.connection.commit()
            logger.info(f"Loaded {loaded} rows into dim_customer")
            return True
        except Exception as e:
            logger.error(f"Failed to load dim_customer: {e}")
            self.connection.rollback()
            return False

    # ================================================================
    # LOAD fact_sales (incremental - only new rows)
    # ================================================================
    def load_fact_sales(self, df, last_loaded_date):
        logger.info(f"Loading fact_sales — records after {last_loaded_date}...")
        try:
            # Filter only NEW records using watermark
            new_df = df[df['InvoiceDate'] > last_loaded_date]
            logger.info(f"Found {len(new_df)} new rows to load")

            if len(new_df) == 0:
                logger.info("No new data to load — pipeline is up to date!")
                return True

            cursor = self.connection.cursor()

            for i, row in new_df.iterrows():
                try:
                    # Get foreign keys from dimension tables
                    cursor.execute("SELECT ProductKey FROM dbo.dim_product WHERE StockCode = ?", row['StockCode'])
                    product_row = cursor.fetchone()

                    cursor.execute("SELECT CustomerKey FROM dbo.dim_customer WHERE CustomerID = ?", int(row['CustomerID']))
                    customer_row = cursor.fetchone()

                    if not product_row or not customer_row:
                        logger.warning(f"Skipping row {i} — missing dimension key")
                        continue

                    cursor.execute("""
                        INSERT INTO dbo.fact_sales
                            (InvoiceNo, DateKey, ProductKey, CustomerKey,
                             Quantity, UnitPrice, TotalSalesAmount, Country)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    row['InvoiceNo'], row['DateKey'],
                    product_row[0], customer_row[0],
                    int(row['Quantity']), float(row['UnitPrice']),
                    float(row['TotalSalesAmount']), row['Country'])

                except Exception as e:
                    logger.warning(f"Skipping fact row {i}: {e}")
                    continue

                # Batch commit every 10k rows
                if (i + 1) % 10000 == 0:
                    self.connection.commit()
                    logger.info(f"Committed {i + 1} rows...")

            self.connection.commit()
            logger.info(f"Loaded {len(new_df)} rows into fact_sales")
            return True

        except Exception as e:
            logger.error(f"Failed to load fact_sales: {e}")
            self.connection.rollback()
            return False