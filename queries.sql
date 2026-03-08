-- ============================================================================
-- E-COMMERCE ETL PIPELINE - ANALYSIS QUERIES
-- Run these queries after ETL pipeline completes
-- ============================================================================

USE EcommerceETL;
GO

-- ============================================================================
-- 1. OVERALL METRICS
-- ============================================================================

-- Total revenue and transaction summary
SELECT 
    COUNT(DISTINCT InvoiceNo) AS TotalTransactions,
    COUNT(DISTINCT CustomerID) AS UniqueCustomers,
    COUNT(DISTINCT StockCode) AS UniqueProducts,
    SUM(TotalSalesAmount) AS TotalRevenue,
    AVG(TotalSalesAmount) AS AvgTransactionValue,
    MIN(InvoiceDate) AS EarliestDate,
    MAX(InvoiceDate) AS LatestDate
FROM dbo.Transactions;
GO

-- ============================================================================
-- 2. CUSTOMER ANALYSIS
-- ============================================================================

-- Top 20 customers by total spending
SELECT TOP 20
    c.CustomerID,
    c.Country,
    c.TotalPurchases,
    c.FirstPurchaseDate,
    c.LastPurchaseDate,
    DATEDIFF(DAY, c.FirstPurchaseDate, c.LastPurchaseDate) AS CustomerLifespanDays,
    SUM(t.TotalSalesAmount) AS TotalSpent,
    AVG(t.TotalSalesAmount) AS AvgOrderValue,
    COUNT(DISTINCT t.InvoiceNo) AS NumberOfOrders
FROM dbo.Customers c
JOIN dbo.Transactions t ON c.CustomerID = t.CustomerID
GROUP BY c.CustomerID, c.Country, c.TotalPurchases, c.FirstPurchaseDate, c.LastPurchaseDate
ORDER BY TotalSpent DESC;
GO

-- Customer segmentation by spending
WITH CustomerSpending AS (
    SELECT 
        c.CustomerID,
        c.Country,
        SUM(t.TotalSalesAmount) AS TotalSpent
    FROM dbo.Customers c
    JOIN dbo.Transactions t ON c.CustomerID = t.CustomerID
    GROUP BY c.CustomerID, c.Country
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
    p.StockCode,
    p.Description,
    COUNT(DISTINCT t.InvoiceNo) AS TimesPurchased,
    SUM(t.Quantity) AS TotalQuantitySold,
    AVG(t.UnitPrice) AS AvgPrice,
    SUM(t.TotalSalesAmount) AS TotalRevenue,
    SUM(t.TotalSalesAmount) / NULLIF(COUNT(DISTINCT t.InvoiceNo), 0) AS RevenuePerTransaction
FROM dbo.Products p
JOIN dbo.Transactions t ON p.StockCode = t.StockCode
GROUP BY p.StockCode, p.Description
ORDER BY TotalRevenue DESC;
GO

-- Product pricing analysis
SELECT TOP 30
    StockCode,
    Description,
    MIN(UnitPrice) AS MinPrice,
    MAX(UnitPrice) AS MaxPrice,
    AVG(UnitPrice) AS AvgPrice,
    STDEV(UnitPrice) AS PriceStdDev,
    COUNT(*) AS TransactionCount
FROM dbo.Transactions
GROUP BY StockCode, Description
HAVING COUNT(*) > 5
ORDER BY PriceStdDev DESC;
GO

-- ============================================================================
-- 4. GEOGRAPHIC ANALYSIS
-- ============================================================================

-- Revenue by country (Top 15)
SELECT TOP 15
    Country,
    COUNT(DISTINCT CustomerID) AS UniqueCustomers,
    COUNT(DISTINCT InvoiceNo) AS NumberOfOrders,
    COUNT(*) AS LineItems,
    SUM(TotalSalesAmount) AS TotalRevenue,
    AVG(TotalSalesAmount) AS AvgOrderValue,
    SUM(Quantity) AS TotalUnits
FROM dbo.Transactions
GROUP BY Country
ORDER BY TotalRevenue DESC;
GO

-- ============================================================================
-- 5. TEMPORAL ANALYSIS
-- ============================================================================

-- Monthly revenue trend
SELECT 
    InvoiceYear,
    InvoiceMonth,
    DATEFROMPARTS(InvoiceYear, InvoiceMonth, 1) AS MonthStart,
    COUNT(DISTINCT InvoiceNo) AS Orders,
    COUNT(DISTINCT CustomerID) AS UniqueCustomers,
    SUM(TotalSalesAmount) AS TotalRevenue,
    AVG(TotalSalesAmount) AS AvgOrderValue
FROM dbo.Transactions
GROUP BY InvoiceYear, InvoiceMonth
ORDER BY InvoiceYear, InvoiceMonth;
GO

-- Daily revenue patterns (Day of week)
SELECT 
    DATEPART(WEEKDAY, InvoiceDate) AS DayOfWeek,
    CASE DATEPART(WEEKDAY, InvoiceDate)
        WHEN 1 THEN 'Sunday'
        WHEN 2 THEN 'Monday'
        WHEN 3 THEN 'Tuesday'
        WHEN 4 THEN 'Wednesday'
        WHEN 5 THEN 'Thursday'
        WHEN 6 THEN 'Friday'
        WHEN 7 THEN 'Saturday'
    END AS DayName,
    COUNT(DISTINCT InvoiceNo) AS Orders,
    SUM(TotalSalesAmount) AS TotalRevenue,
    AVG(TotalSalesAmount) AS AvgOrderValue
FROM dbo.Transactions
GROUP BY DATEPART(WEEKDAY, InvoiceDate)
ORDER BY DayOfWeek;
GO

-- ============================================================================
-- 6. DATA QUALITY CHECKS
-- ============================================================================

-- Check for any anomalies or data quality issues
SELECT 
    'Total Transactions' AS QualityCheck,
    COUNT(*) AS RecordCount
FROM dbo.Transactions
UNION ALL
SELECT 'Unique Customers', COUNT(DISTINCT CustomerID) FROM dbo.Transactions
UNION ALL
SELECT 'Unique Products', COUNT(DISTINCT StockCode) FROM dbo.Transactions
UNION ALL
SELECT 'Transactions with NULL CustomerID', COUNT(*) FROM dbo.Transactions WHERE CustomerID IS NULL
UNION ALL
SELECT 'Transactions with NULL Country', COUNT(*) FROM dbo.Transactions WHERE Country IS NULL
UNION ALL
SELECT 'Invalid Quantities (<=0)', COUNT(*) FROM dbo.Transactions WHERE Quantity <= 0
UNION ALL
SELECT 'Invalid Prices (<=0)', COUNT(*) FROM dbo.Transactions WHERE UnitPrice <= 0
UNION ALL
SELECT 'Negative Sales Amounts', COUNT(*) FROM dbo.Transactions WHERE TotalSalesAmount < 0;
GO

-- ============================================================================
-- 7. CUSTOMER RETENTION ANALYSIS
-- ============================================================================

-- Repeat customer analysis - how many customers made X number of purchases
WITH CustomerOrders AS (
    SELECT 
        CustomerID,
        COUNT(DISTINCT InvoiceNo) AS OrderCount
    FROM dbo.Transactions
    GROUP BY CustomerID
)
SELECT 
    OrderCount AS NumberOfOrders,
    COUNT(*) AS NumberOfCustomers,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS PercentageOfCustomers
FROM CustomerOrders
GROUP BY OrderCount
ORDER BY OrderCount;
GO

-- ============================================================================
-- 8. VIEWS FOR ANALYTICS (Optional - uncomment to create)
-- ============================================================================

-- Daily sales summary view
CREATE OR ALTER VIEW vw_DailySalesSummary AS
SELECT 
    CAST(InvoiceDate AS DATE) AS SalesDate,
    Country,
    COUNT(DISTINCT InvoiceNo) AS DailyOrders,
    COUNT(DISTINCT CustomerID) AS DailyUniqueCustomers,
    SUM(Quantity) AS DailyQuantity,
    SUM(TotalSalesAmount) AS DailyRevenue,
    AVG(TotalSalesAmount) AS AvgDailyOrderValue
FROM dbo.Transactions
GROUP BY CAST(InvoiceDate AS DATE), Country;
GO

-- Product performance view
CREATE OR ALTER VIEW vw_ProductPerformance AS
SELECT 
    p.StockCode,
    p.Description,
    SUM(t.Quantity) AS TotalQuantitySold,
    SUM(t.TotalSalesAmount) AS TotalRevenue,
    COUNT(DISTINCT t.InvoiceNo) AS TimesPurchased,
    AVG(t.UnitPrice) AS AvgUnitPrice,
    COUNT(DISTINCT t.CustomerID) AS UniqueCustomersBought
FROM dbo.Products p
LEFT JOIN dbo.Transactions t ON p.StockCode = t.StockCode
GROUP BY p.StockCode, p.Description;
GO

-- Customer summary view
CREATE OR ALTER VIEW vw_CustomerSummary AS
SELECT 
    c.CustomerID,
    c.Country,
    c.FirstPurchaseDate,
    c.LastPurchaseDate,
    DATEDIFF(DAY, c.FirstPurchaseDate, c.LastPurchaseDate) AS CustomerLifespanDays,
    COUNT(DISTINCT t.InvoiceNo) AS TotalOrders,
    SUM(t.Quantity) AS TotalItemsPurchased,
    SUM(t.TotalSalesAmount) AS TotalSpent,
    AVG(t.TotalSalesAmount) AS AvgOrderValue
FROM dbo.Customers c
LEFT JOIN dbo.Transactions t ON c.CustomerID = t.CustomerID
GROUP BY c.CustomerID, c.Country, c.FirstPurchaseDate, c.LastPurchaseDate;
GO

PRINT 'All analysis queries and views created successfully!'
GO
