-- ============================================================
-- STAR SCHEMA for E-Commerce ETL Pipeline
-- ============================================================
USE data_engineering;
GO

-- Drop tables if they exist (in correct order)
IF OBJECT_ID('dbo.fact_sales', 'U') IS NOT NULL DROP TABLE dbo.fact_sales;
IF OBJECT_ID('dbo.dim_product', 'U') IS NOT NULL DROP TABLE dbo.dim_product;
IF OBJECT_ID('dbo.dim_customer', 'U') IS NOT NULL DROP TABLE dbo.dim_customer;
IF OBJECT_ID('dbo.dim_date', 'U') IS NOT NULL DROP TABLE dbo.dim_date;
IF OBJECT_ID('dbo.watermark', 'U') IS NOT NULL DROP TABLE dbo.watermark;

-- ============================================================
-- DIMENSION: dim_date
-- ============================================================
CREATE TABLE dbo.dim_date (
    DateKey         INT PRIMARY KEY,        -- e.g. 20101201
    FullDate        DATE NOT NULL,
    Year            INT NOT NULL,
    Month           INT NOT NULL,
    MonthName       VARCHAR(20) NOT NULL,
    Quarter         INT NOT NULL,
    Day             INT NOT NULL,
    DayOfWeek       VARCHAR(20) NOT NULL,
    IsWeekend       BIT NOT NULL
);

-- ============================================================
-- DIMENSION: dim_product
-- ============================================================
CREATE TABLE dbo.dim_product (
    ProductKey      INT IDENTITY(1,1) PRIMARY KEY,
    StockCode       VARCHAR(50) NOT NULL UNIQUE,
    Description     VARCHAR(255)
);

-- ============================================================
-- DIMENSION: dim_customer
-- ============================================================
CREATE TABLE dbo.dim_customer (
    CustomerKey     INT IDENTITY(1,1) PRIMARY KEY,
    CustomerID      INT NOT NULL UNIQUE,
    Country         VARCHAR(100),
    FirstPurchaseDate DATE,
    LastPurchaseDate  DATE
);

-- ============================================================
-- FACT TABLE: fact_sales
-- ============================================================
CREATE TABLE dbo.fact_sales (
    SalesKey        INT IDENTITY(1,1) PRIMARY KEY,
    InvoiceNo       VARCHAR(20) NOT NULL,
    DateKey         INT NOT NULL,
    ProductKey      INT NOT NULL,
    CustomerKey     INT NOT NULL,
    Quantity        INT NOT NULL,
    UnitPrice       DECIMAL(10,2) NOT NULL,
    TotalSalesAmount DECIMAL(10,2) NOT NULL,
    Country         VARCHAR(100),

    FOREIGN KEY (DateKey)     REFERENCES dbo.dim_date(DateKey),
    FOREIGN KEY (ProductKey)  REFERENCES dbo.dim_product(ProductKey),
    FOREIGN KEY (CustomerKey) REFERENCES dbo.dim_customer(CustomerKey)
);

-- ============================================================
-- WATERMARK TABLE (tracks last loaded date for incremental load)
-- ============================================================
CREATE TABLE dbo.watermark (
    TableName       VARCHAR(100) PRIMARY KEY,
    LastLoadedDate  DATETIME NOT NULL
);

-- Seed the watermark with a start date
INSERT INTO dbo.watermark (TableName, LastLoadedDate)
VALUES ('fact_sales', '2010-01-01 00:00:00');

--to check only
--USE data_engineering;
--SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE';