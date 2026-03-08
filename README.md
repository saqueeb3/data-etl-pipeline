# E-Commerce ETL Pipeline

## What Is This?

I built an **ETL pipeline** that takes 540K+ messy e-commerce transactions from a CSV file and loads them into a clean SQL Server database.

**Extract → Transform → Load**

That's it. That's the whole thing.

------------------------------------------------------------------------------------------------------------------------------------------------------------------

## The Numbers

- **Raw data:** 541,909 rows
- **After cleaning:** 392,692 rows  
- **Products:** 3,665
- **Customers:** 4,338
- **Processing time:** ~4 minutes

------------------------------------------------------------------------------------------------------------------------------------------------------------------

## What It Actually Does

**EXTRACT:** Reads the CSV file (had to use Latin-1 encoding because of course it wasn't UTF-8)

**TRANSFORM:** Removes all the garbage:
- Duplicates
- Cancelled orders
- Missing critical data
- Invalid quantities/prices
- Whitespace in text fields

**LOAD:** Puts the clean data into SQL Server with proper relationships

------------------------------------------------------------------------------------------------------------------------------------------------------------------

## How To Run It

### You need:
- Python 3.8+
- SQL Server Express
- The Kaggle "Online Retail" dataset (download it and put it in `data/raw/data.csv`)

### Steps:
1. `pip install -r requirements.txt`
2. Create database: `CREATE DATABASE EcommerceETL;`
3. Update SERVER name in `scripts/main.py` 
4. Run: `python main.py`

Done. It'll take like 4 minutes.

------------------------------------------------------------------------------------------------------------------------------------------------------------------

## Tech Stack

- Python (pandas, pyodbc)
- SQL Server Express 2025
- Windows Authentication (because I'm cheap and don't want to manage passwords)

------------------------------------------------------------------------------------------------------------------------------------------------------------------

## The Annoying Challenges I Fixed

### 1. Encoding Nightmare
**Problem:** `'utf-8' codec can't decode byte 0xa3`  
**Fix:** Added `encoding='latin-1'` to read the CSV  
**Lesson:** Different datasets use different encodings. Who knew? Not me.

### 2. Foreign Key Errors
**Problem:** Tried deleting Products before Transactions (oops, foreign key relationship)  
**Fix:** Delete in correct order - Transactions → Customers → Products  
**Lesson:** Database relationships matter. A lot.

### 3. Duplicate Products
**Problem:** Same product appeared multiple times  
**Fix:** Used `drop_duplicates(subset=['StockCode'], keep='first')`  
**Lesson:** Real data is messy.

### 4. Memory Issues with 392K Rows
**Problem:** Slow and memory-intensive  
**Fix:** Batch commits every 10k rows  
**Lesson:** For large datasets, batch processing is your friend.

------------------------------------------------------------------------------------------------------------------------------------------------------------------

## Code Quality

**Important note:** The code has **VERY SARCASTIC COMMENTS** throughout because I was frustrated while debugging. Comments like:

- "thank god pandas library"
- "do i know what UTF-8 is? NO!"
- "so i don't cry in SQL"
- "no redundancy in my kingdom"

I left these in because:
	umm, i suffered, and at the end when it showed successful, I felt happiness.
	--also because my elder cousin told keep it, it's your journey 

If you read the code, you'll see my actual thought process and struggles. That's the point.

------------------------------------------------------------------------------------------------------------------------------------------------------------------

## Project Structure

```
data-etl-project/
├── scripts/
│   ├── main.py              # Orchestrates everything
│   ├── extract.py           # Reads CSV
│   ├── transform.py         # Cleans data
│   └── load.py              # Inserts into database
├── sql/
│   ├── schema.sql                     # Creates tables
│   └── queries.sql                    # Analysis queries
│   └── SQL_Analysis_run to test.sql   #to test the DB      
├── data/raw/                          # Put your data.csv here
└── requirements.txt
```

------------------------------------------------------------------------------------------------------------------------------------------------------------------

## What I Learned

** ETL pipeline design and execution  
** How to handle real-world messy data  
** SQL Server foreign key constraints  
** Batch processing for large datasets  
** Python pandas for data transformation  
** Proper error handling and logging  
**That data quality is WAY harder than it looks  

------------------------------------------------------------------------------------------------------------------------------------------------------------------

## The Queries

In `sql/queries.sql` you'll find 20+ queries to analyze:
- Top customers by revenue
- Revenue by country  
- Product performance
- Monthly trends
- Data quality checks

Just run them in SQL Server Management Studio.

------------------------------------------------------------------------------------------------------------------------------------------------------------------

## Future Ideas

- Incremental loads (only new data)
- Scheduled execution with Task Scheduler
- Power BI dashboards(if i decided to actually learn it)
- Better error notifications

But honestly? This works. It's done. It processes data. Mission accomplished.

------------------------------------------------------------------------------------------------------------------------------------------------------------------

**Built by:** Saqueeb  
**When:** March 2026  
**Why:** Portfolio project + Actually needed to learn this stuff  
**Status:** Working. Actually processes the data and doesn't crash.

------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

*P.S. - If you use this as a template, you'll learn more from reading the actual code comments than from this README. The comments are where the real story is.*
------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
