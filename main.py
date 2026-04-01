"""
E-Commerce ETL Pipeline — Star Schema Edition
Orchestrates Extract → Transform → Load into star schema with incremental loading
"""

import logging
import sys
from datetime import datetime

from extract import extract_data
from transform import transform_data, generate_date_dimension
from load_star import StarSchemaLoader

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('etl_pipeline.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# ============================================================
# Configuration
# ============================================================
from config import CSV_FILE, SERVER, DATABASE

# ============================================================
# Main Pipeline
# ============================================================
def run_etl_pipeline():

    logger.info("=" * 70)
    logger.info("E-Commerce ETL Pipeline — Star Schema + Incremental Load")
    logger.info("=" * 70)

    start_time = datetime.now()

    # -------------------------
    # PHASE 1: EXTRACT
    # -------------------------
    logger.info("[PHASE 1] EXTRACT")
    logger.info("-" * 70)
    df = extract_data(CSV_FILE)
    if df is None:
        logger.error("Pipeline failed at extraction stage")
        return False
    logger.info("Extraction completed successfully")

    # -------------------------
    # PHASE 2: TRANSFORM
    # -------------------------
    logger.info("[PHASE 2] TRANSFORM")
    logger.info("-" * 70)
    df = transform_data(df)
    if df is None:
        logger.error("Pipeline failed at transformation stage")
        return False

    dim_date_df = generate_date_dimension(df)
    if dim_date_df is None:
        logger.error("Pipeline failed generating date dimension")
        return False
    logger.info("Transformation completed successfully")

    # -------------------------
    # PHASE 3: LOAD
    # -------------------------
    logger.info("[PHASE 3] LOAD")
    logger.info("-" * 70)

    loader = StarSchemaLoader(server=SERVER, database=DATABASE)

    if not loader.connect():
        logger.error("Pipeline failed to connect to SQL Server")
        return False

    try:
        # Get watermark — only load records newer than this
        last_loaded = loader.get_watermark()
        logger.info(f"Watermark — last loaded date: {last_loaded}")

        # Load dimensions first
        if not loader.load_dim_date(dim_date_df):
            logger.error("Failed to load dim_date")
            return False

        if not loader.load_dim_product(df):
            logger.error("Failed to load dim_product")
            return False

        if not loader.load_dim_customer(df):
            logger.error("Failed to load dim_customer")
            return False

        # Load fact table — incremental only
        if not loader.load_fact_sales(df, last_loaded):
            logger.error("Failed to load fact_sales")
            return False

        # Update watermark to latest date in this batch
        new_watermark = df['InvoiceDate'].max()
        loader.update_watermark(new_watermark)

    finally:
        loader.disconnect()

    duration = datetime.now() - start_time
    logger.info("=" * 70)
    logger.info("ETL Pipeline completed successfully!")
    logger.info(f"Total duration: {duration}")
    logger.info("=" * 70)
    return True


if __name__ == "__main__":
    success = run_etl_pipeline()
    sys.exit(0 if success else 1)
