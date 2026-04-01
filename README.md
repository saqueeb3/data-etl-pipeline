# E-Commerce ETL Pipeline — Star Schema Edition

[![Python](https://img.shields.io/badge/Python-3.14-blue)](https://www.python.org/)
[![SQL Server](https://img.shields.io/badge/SQL%20Server-2025%20Express-red)](https://www.microsoft.com/sql-server)
[![Status](https://img.shields.io/badge/Status-Production%20Ready-green)]()

> A production-grade **ETL pipeline** that extracts e-commerce transaction data, transforms it with rigorous data quality checks, and loads it into a **star schema** optimized for analytics. Features **watermark-based incremental loading** for efficient re-runs.

## Results

| Metric | Value |
|--------|-------|
| **Raw Data** | 541,909 rows |
| **Cleaned Data** | 392,692 rows (72.5%) |
| **End-to-End Time** | ~27 seconds |
| **Incremental Runs** | <5 seconds |
| **Dimensions Loaded** | 305 dates, 3,665 products, 4,338 customers |
| **Facts Loaded** | 392,692 transactions |

## Architecture

```
CSV (Kaggle Online Retail Dataset)
    ↓ extract.py (Latin-1 encoding)
    ↓ 541,909 rows
    ↓
transform.py (11-step cleaning pipeline)
    • Remove nulls in critical columns
    • Drop duplicates
    • Validate dates & currency amounts
    • Filter cancelled orders
    • Extract temporal components
    • Create DateKey for star joins
    ↓ 392,692 clean rows
    ↓
load_star.py (dimension-first, incremental fact load)
    ↓
SQL Server: data_engineering database
    ├── dim_date (305 records)
    ├── dim_product (3,665 records)
    ├── dim_customer (4,338 records)
    ├── fact_sales (392,692 records)
    └── watermark (LastLoadedDate tracking)
```

## Star Schema Design

**Dimension Tables:**
- **dim_date** — 9 attributes: Year, Month, MonthName, Quarter, Day, DayOfWeek, IsWeekend
- **dim_product** — ProductKey (surrogate), StockCode, Description
- **dim_customer** — CustomerKey (surrogate), CustomerID, Country, FirstPurchaseDate, LastPurchaseDate
- **fact_sales** — InvoiceNo, DateKey, ProductKey, CustomerKey, Quantity, UnitPrice, TotalSalesAmount, Country

**Incremental Loading:**
- Watermark table tracks `LastLoadedDate` for `fact_sales`
- Only inserts records with `InvoiceDate > LastLoadedDate`
- Dimensions use "IF NOT EXISTS" to prevent duplicates
- Batch commits every 10K rows for performance

## Setup

### Requirements
```bash
pip install -r requirements.txt
```

### Configuration
Create `.env` file in project root:
```
CSV_FILE=D:\Data Engineering\data-etl-project\data\raw\data.csv
SQL_SERVER=localhost\SQLEXPRESS
SQL_DATABASE=data_engineering
```

### Database Schema
Run in **SQL Server Management Studio**:
```sql
-- Execute schema_star.sql to create tables, indexes, and watermark
```

## Running the Pipeline

```bash
python main.py
```

**Output:**
```
======================================================================
E-Commerce ETL Pipeline — Star Schema + Incremental Load
======================================================================
[PHASE 1] EXTRACT
...Successfully extracted 541909 rows...

[PHASE 2] TRANSFORM
...Rows after removing nulls in critical columns: 406829...
...Removed 5225 duplicate rows...
...Final transformed row count: 392692...

[PHASE 3] LOAD
...Connected to data_engineering on localhost\SQLEXPRESS...
...Loaded 305 rows into dim_date...
...Loaded 3665 rows into dim_product...
...Loaded 4338 rows into dim_customer...
...Loaded 392692 rows into fact_sales...
...Watermark updated to 2011-12-09 12:50:00...

ETL Pipeline completed successfully!
Total duration: 0:00:27.680858
======================================================================
```

Logs are saved to `etl_pipeline.log`.

## Project Files

| File | Purpose |
|------|---------|
| `extract.py` | Reads CSV with proper encoding handling |
| `transform.py` | 11-step data cleaning & validation |
| `load_star.py` | Star schema loader with watermark logic |
| `main.py` | ETL orchestrator (phases + logging) |
| `config.py` | Environment variable management |
| `schema_star.sql` | DDL for star schema tables |
| `queries_star.sql` | Analysis queries (with views) |
| `requirements.txt` | Python dependencies |

## 🔍 Data Quality

**Transformations Applied:**
1. Remove NULLs in InvoiceNo, StockCode, CustomerID (135,080 rows removed)
2. Drop exact duplicates (5,225 rows removed)
3. Parse InvoiceDate to datetime (MM/DD/YYYY HH:MM format)
4. Filter cancelled orders (C-prefix in InvoiceNo, 8,872 rows removed)
5. Remove invalid quantities & prices ≤0 (40 rows removed)
6. Calculate TotalSalesAmount (Quantity × UnitPrice)
7. Trim whitespace from strings
8. Cast CustomerID to integer type
9. Extract temporal components (Year, Month, Day)
10. Create DateKey for dimension joins (YYYYMMDD format)
11. Generate date dimension with 9 attributes

**Quality Checks:**
- No NULLs in fact_sales critical columns
- No orphaned foreign keys
- All dates within dataset range (2010-12-01 to 2011-12-09)
- All quantities and prices > 0

## 📈 Sample Queries

See `queries_star.sql` for complete examples:

```sql
-- Top 20 customers by revenue
SELECT TOP 20
    dc.CustomerID, dc.Country,
    SUM(fs.TotalSalesAmount) AS TotalSpent
FROM dbo.dim_customer dc
JOIN dbo.fact_sales fs ON dc.CustomerKey = fs.CustomerKey
GROUP BY dc.CustomerID, dc.Country
ORDER BY TotalSpent DESC;

-- Monthly revenue trend
SELECT 
    dd.Year, dd.Month, dd.MonthName,
    COUNT(DISTINCT fs.InvoiceNo) AS Orders,
    SUM(fs.TotalSalesAmount) AS Revenue
FROM dbo.fact_sales fs
JOIN dbo.dim_date dd ON fs.DateKey = dd.DateKey
GROUP BY dd.Year, dd.Month, dd.MonthName
ORDER BY dd.Year, dd.Month;
```

## Technical Stack

- **Language:** Python 3.14
- **Data Processing:** pandas 2.0.3
- **Database:** SQL Server 2025 Express
- **Driver:** ODBC Driver 17 for SQL Server
- **Configuration:** python-dotenv 1.0.0
- **Logging:** Python built-in logging

## Next Steps

- [ ] Integrate with **Power BI** for interactive dashboards
- [ ] Add **dbt** transformations for data mart layer
- [ ] Schedule with **Windows Task Scheduler** or **Azure Data Factory**
- [ ] Create monitoring alerts for ETL failures
- [ ] Extend to real-time incremental loads with Kafka

## Key Design Decisions

**Why Star Schema?**
- Optimized for analytics (denormalized dimensions, fact-driven queries)
- Easy to understand dimensional relationships
- Fast joins on dimension keys
- Supports incremental loading pattern

**Why Watermark-Based Loading?**
- Avoids full table scans on re-runs
- Enables scheduled ETL jobs (every hour/day)
- Tracks data freshness transparently
- Prevents duplicate fact records

**Why Surrogate Keys?**
- Decouples business keys from technical keys
- Allows dimension updates (slowly changing dimensions)
- Improves query performance
- Supports audit trails

## Data Source

Dataset: **Kaggle Online Retail** (E-commerce transactions, Dec 2010 – Dec 2011)
- 541,909 transactions
- 8 original columns (InvoiceNo, StockCode, Description, Quantity, InvoiceDate, UnitPrice, CustomerID, Country)

## License

MIT

---

**Built by:** Saqueeb Akhtar Ansari  
**Portfolio:** [github.com/saqueeb3](https://github.com/saqueeb3) | [linkedin.com/in/saqueeb-akhtar-ansari](https://linkedin.com/in/saqueeb-akhtar-ansari-b25891389)
