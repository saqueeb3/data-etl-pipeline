USE EcommerceETL;
SELECT TOP 20
    c.CustomerID,
    c.Country,
    SUM(t.TotalSalesAmount) AS TotalSpent
FROM dbo.Customers c
JOIN dbo.Transactions t ON c.CustomerID = t.CustomerID
GROUP BY c.CustomerID, c.Country
ORDER BY TotalSpent DESC;


SELECT TOP 15
    Country,
    COUNT(DISTINCT CustomerID) AS UniqueCustomers,
    SUM(TotalSalesAmount) AS TotalRevenue
FROM dbo.Transactions
GROUP BY Country
ORDER BY TotalRevenue DESC;


SELECT TOP 20
    p.StockCode,
    p.Description,
    COUNT(DISTINCT t.InvoiceNo) AS TimesPurchased,
    SUM(t.TotalSalesAmount) AS TotalRevenue
FROM dbo.Products p
JOIN dbo.Transactions t ON p.StockCode = t.StockCode
GROUP BY p.StockCode, p.Description
ORDER BY TotalRevenue DESC;