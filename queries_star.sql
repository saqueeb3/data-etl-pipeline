-- ============================================================================
-- E-COMMERCE ETL PIPELINE - STAR SCHEMA ANALYSIS QUERIES
-- Use these ONLY after running the star schema pipeline
-- ============================================================================

USE data_engineering;
GO

-- ============================================================================
-- 1. OVERALL METRICS
-- ============================================================================

-- Total revenue and transaction summary using STAR SCHEMA
SELECT 
    COUNT(DISTINCT fs.InvoiceNo) AS TotalTransactions,
    COUNT(DISTINCT fs.CustomerKey) AS UniqueCustomers,
    COUNT(DISTINCT fs.ProductKey) AS UniqueProducts,
    SUM(fs.TotalSalesAmount) AS TotalRevenue,
    AVG(fs.TotalSalesAmount) AS AvgTransactionValue,
    MIN(dd.FullDate) AS EarliestDate,
    MAX(dd.FullDate) AS LatestDate
FROM dbo.fact_sales fs
JOIN dbo.dim_date dd ON fs.DateKey = dd.DateKey;
GO

-- ============================================================================
-- 2. CUSTOMER ANALYSIS
-- ============================================================================

-- Top 20 customers by total spending
SELECT TOP 20
    dc.CustomerID,
    dc.Country,
    dc.FirstPurchaseDate,
    dc.LastPurchaseDate,
    DATEDIFF(DAY, dc.FirstPurchaseDate, dc.LastPurchaseDate) AS CustomerLifespanDays,
    SUM(fs.TotalSalesAmount) AS TotalSpent,
    AVG(fs.TotalSalesAmount) AS AvgOrderValue,
    COUNT(DISTINCT fs.InvoiceNo) AS NumberOfOrders
FROM dbo.dim_customer dc
JOIN dbo.fact_sales fs ON dc.CustomerKey = fs.CustomerKey
GROUP BY dc.CustomerID, dc.Country, dc.FirstPurchaseDate, dc.LastPurchaseDate
ORDER BY TotalSpent DESC;
GO

-- Customer segmentation by spending
WITH CustomerSpending AS (
    SELECT 
        dc.CustomerID,
        dc.Country,
        SUM(fs.TotalSalesAmount) AS TotalSpent
    FROM dbo.dim_customer dc
    JOIN dbo.fact_sales fs ON dc.CustomerKey = fs.CustomerKey
    GROUP BY dc.CustomerID, dc.Country
)
SELECT 
    CASE 
        WHEN TotalSpent > 5000 THEN 'High Value'
        WHEN TotalSpent > 1000 THEN 'Medium Value'
        ELSE 'Low Value'
    END AS CustomerSegment,
    COUNT(*) AS NumberOfCustomers,
    AVG(TotalSpent) AS AvgSpentPerSegment,
    MIN(TotalSpent) AS MinSpent,
    MAX(TotalSpent) AS MaxSpent
FROM CustomerSpending
GROUP BY 
    CASE 
        WHEN TotalSpent > 5000 THEN 'High Value'
        WHEN TotalSpent > 1000 THEN 'Medium Value'
        ELSE 'Low Value'
    END
ORDER BY AvgSpentPerSegment DESC;
GO

-- ============================================================================
-- 3. PRODUCT ANALYSIS
-- ============================================================================

-- Top 20 products by revenue
SELECT TOP 20
    dp.StockCode,
    dp.Description,
    COUNT(DISTINCT fs.InvoiceNo) AS TimesPurchased,
    SUM(fs.Quantity) AS TotalQuantitySold,
    AVG(fs.UnitPrice) AS AvgPrice,
    SUM(fs.TotalSalesAmount) AS TotalRevenue,
    SUM(fs.TotalSalesAmount) / NULLIF(COUNT(DISTINCT fs.InvoiceNo), 0) AS RevenuePerTransaction
FROM dbo.dim_product dp
JOIN dbo.fact_sales fs ON dp.ProductKey = fs.ProductKey
GROUP BY dp.StockCode, dp.Description
ORDER BY TotalRevenue DESC;
GO

-- Product pricing analysis
SELECT TOP 30
    dp.StockCode,
    dp.Description,
    MIN(fs.UnitPrice) AS MinPrice,
    MAX(fs.UnitPrice) AS MaxPrice,
    AVG(fs.UnitPrice) AS AvgPrice,
    STDEV(fs.UnitPrice) AS PriceStdDev,
    COUNT(*) AS TransactionCount
FROM dbo.dim_product dp
JOIN dbo.fact_sales fs ON dp.ProductKey = fs.ProductKey
GROUP BY dp.StockCode, dp.Description
HAVING COUNT(*) > 5
ORDER BY PriceStdDev DESC;
GO

-- ============================================================================
-- 4. GEOGRAPHIC ANALYSIS
-- ============================================================================

-- Revenue by country (Top 15)
SELECT TOP 15
    fs.Country,
    COUNT(DISTINCT fs.CustomerKey) AS UniqueCustomers,
    COUNT(DISTINCT fs.InvoiceNo) AS NumberOfOrders,
    COUNT(*) AS LineItems,
    SUM(fs.TotalSalesAmount) AS TotalRevenue,
    AVG(fs.TotalSalesAmount) AS AvgOrderValue,
    SUM(fs.Quantity) AS TotalUnits
FROM dbo.fact_sales fs
GROUP BY fs.Country
ORDER BY TotalRevenue DESC;
GO

-- ============================================================================
-- 5. TEMPORAL ANALYSIS
-- ============================================================================

-- Monthly revenue trend using dim_date
SELECT 
    dd.Year,
    dd.Month,
    dd.MonthName,
    COUNT(DISTINCT fs.InvoiceNo) AS Orders,
    COUNT(DISTINCT fs.CustomerKey) AS UniqueCustomers,
    SUM(fs.TotalSalesAmount) AS TotalRevenue,
    AVG(fs.TotalSalesAmount) AS AvgOrderValue
FROM dbo.fact_sales fs
JOIN dbo.dim_date dd ON fs.DateKey = dd.DateKey
GROUP BY dd.Year, dd.Month, dd.MonthName
ORDER BY dd.Year, dd.Month;
GO

-- Daily revenue patterns (Day of week)
SELECT 
    dd.DayOfWeek,
    COUNT(DISTINCT fs.InvoiceNo) AS Orders,
    SUM(fs.TotalSalesAmount) AS TotalRevenue,
    AVG(fs.TotalSalesAmount) AS AvgOrderValue
FROM dbo.fact_sales fs
JOIN dbo.dim_date dd ON fs.DateKey = dd.DateKey
GROUP BY dd.DayOfWeek
ORDER BY CASE 
    WHEN dd.DayOfWeek = 'Monday' THEN 1
    WHEN dd.DayOfWeek = 'Tuesday' THEN 2
    WHEN dd.DayOfWeek = 'Wednesday' THEN 3
    WHEN dd.DayOfWeek = 'Thursday' THEN 4
    WHEN dd.DayOfWeek = 'Friday' THEN 5
    WHEN dd.DayOfWeek = 'Saturday' THEN 6
    WHEN dd.DayOfWeek = 'Sunday' THEN 7
END;
GO

-- ============================================================================
-- 6. DATA QUALITY CHECKS (STAR SCHEMA)
-- ============================================================================

-- Star schema data quality summary
SELECT 
    'Total fact_sales records' AS QualityCheck,
    COUNT(*) AS RecordCount
FROM dbo.fact_sales
UNION ALL
SELECT 'Unique dates (dim_date)', COUNT(*) FROM dbo.dim_date
UNION ALL
SELECT 'Unique products (dim_product)', COUNT(*) FROM dbo.dim_product
UNION ALL
SELECT 'Unique customers (dim_customer)', COUNT(*) FROM dbo.dim_customer
UNION ALL
SELECT 'Orphaned facts (missing product)', COUNT(*) FROM dbo.fact_sales WHERE ProductKey IS NULL
UNION ALL
SELECT 'Orphaned facts (missing customer)', COUNT(*) FROM dbo.fact_sales WHERE CustomerKey IS NULL
UNION ALL
SELECT 'Orphaned facts (missing date)', COUNT(*) FROM dbo.fact_sales WHERE DateKey IS NULL
UNION ALL
SELECT 'Invalid quantities (<=0)', COUNT(*) FROM dbo.fact_sales WHERE Quantity <= 0
UNION ALL
SELECT 'Invalid prices (<=0)', COUNT(*) FROM dbo.fact_sales WHERE UnitPrice <= 0
UNION ALL
SELECT 'Negative sales amounts', COUNT(*) FROM dbo.fact_sales WHERE TotalSalesAmount < 0;
GO

-- ============================================================================
-- 7. VIEWS FOR ANALYTICS (Optional)
-- ============================================================================

-- Star schema view: Daily sales summary
CREATE OR ALTER VIEW vw_DailySalesSummary AS
SELECT 
    dd.FullDate,
    dd.DayOfWeek,
    fs.Country,
    COUNT(DISTINCT fs.InvoiceNo) AS DailyOrders,
    COUNT(DISTINCT fs.CustomerKey) AS DailyUniqueCustomers,
    SUM(fs.Quantity) AS DailyQuantity,
    SUM(fs.TotalSalesAmount) AS DailyRevenue,
    AVG(fs.TotalSalesAmount) AS AvgDailyOrderValue
FROM dbo.fact_sales fs
JOIN dbo.dim_date dd ON fs.DateKey = dd.DateKey
GROUP BY dd.FullDate, dd.DayOfWeek, fs.Country;
GO

-- Star schema view: Product performance
CREATE OR ALTER VIEW vw_ProductPerformance AS
SELECT 
    dp.StockCode,
    dp.Description,
    SUM(fs.Quantity) AS TotalQuantitySold,
    SUM(fs.TotalSalesAmount) AS TotalRevenue,
    COUNT(DISTINCT fs.InvoiceNo) AS TimesPurchased,
    AVG(fs.UnitPrice) AS AvgUnitPrice,
    COUNT(DISTINCT fs.CustomerKey) AS UniqueCustomersBought
FROM dbo.dim_product dp
LEFT JOIN dbo.fact_sales fs ON dp.ProductKey = fs.ProductKey
GROUP BY dp.StockCode, dp.Description;
GO

-- Star schema view: Customer summary
CREATE OR ALTER VIEW vw_CustomerSummary AS
SELECT 
    dc.CustomerID,
    dc.Country,
    dc.FirstPurchaseDate,
    dc.LastPurchaseDate,
    DATEDIFF(DAY, dc.FirstPurchaseDate, dc.LastPurchaseDate) AS CustomerLifespanDays,
    COUNT(DISTINCT fs.InvoiceNo) AS TotalOrders,
    SUM(fs.Quantity) AS TotalItemsPurchased,
    SUM(fs.TotalSalesAmount) AS TotalSpent,
    AVG(fs.TotalSalesAmount) AS AvgOrderValue
FROM dbo.dim_customer dc
LEFT JOIN dbo.fact_sales fs ON dc.CustomerKey = fs.CustomerKey
GROUP BY dc.CustomerID, dc.Country, dc.FirstPurchaseDate, dc.LastPurchaseDate;
GO

PRINT 'All star schema analysis queries and views created successfully!'
GO