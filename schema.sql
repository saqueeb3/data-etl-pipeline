-- ============================================================================
-- E-COMMERCE ETL PIPELINE - SCHEMA DEFINITION
-- Creates the database structure for transformed data
-- ============================================================================

USE EcommerceETL;
GO

-- ============================================================================
-- Drop existing tables (for fresh load)
-- ============================================================================
IF OBJECT_ID('dbo.Transactions', 'U') IS NOT NULL DROP TABLE dbo.Transactions;
GO
IF OBJECT_ID('dbo.Products', 'U') IS NOT NULL DROP TABLE dbo.Products;
GO
IF OBJECT_ID('dbo.Customers', 'U') IS NOT NULL DROP TABLE dbo.Customers;
GO

-- ============================================================================
-- Products Table (Dimension)
-- Stores unique product information
-- ============================================================================
CREATE TABLE dbo.Products (
    StockCode NVARCHAR(20) PRIMARY KEY,
    Description NVARCHAR(255) NOT NULL,
    CreatedAt DATETIME DEFAULT GETDATE()
);
GO

CREATE INDEX idx_Description ON dbo.Products(Description);
GO

-- ============================================================================
-- Customers Table (Dimension)
-- Stores unique customer information with purchase aggregates
-- ============================================================================
CREATE TABLE dbo.Customers (
    CustomerID INT PRIMARY KEY,
    Country NVARCHAR(50),
    FirstPurchaseDate DATETIME,
    LastPurchaseDate DATETIME,
    TotalPurchases INT DEFAULT 0,
    CreatedAt DATETIME DEFAULT GETDATE()
);
GO

CREATE INDEX idx_Country ON dbo.Customers(Country);
GO

-- ============================================================================
-- Transactions Table (Fact)
-- Stores all transaction details with dimensional attributes
-- ============================================================================
CREATE TABLE dbo.Transactions (
    TransactionID INT PRIMARY KEY IDENTITY(1,1),
    InvoiceNo NVARCHAR(20) NOT NULL,
    StockCode NVARCHAR(20) NOT NULL,
    Quantity INT NOT NULL,
    UnitPrice FLOAT NOT NULL,
    TotalSalesAmount FLOAT NOT NULL,
    InvoiceDate DATETIME NOT NULL,
    InvoiceYear INT,
    InvoiceMonth INT,
    InvoiceDay INT,
    CustomerID INT,
    Country NVARCHAR(50),
    CreatedAt DATETIME DEFAULT GETDATE(),
    FOREIGN KEY (StockCode) REFERENCES dbo.Products(StockCode),
    FOREIGN KEY (CustomerID) REFERENCES dbo.Customers(CustomerID)
);
GO

-- ============================================================================
-- Indexes for query performance
-- ============================================================================
CREATE INDEX idx_InvoiceDate ON dbo.Transactions(InvoiceDate);
GO
CREATE INDEX idx_Country ON dbo.Transactions(Country);
GO
CREATE INDEX idx_CustomerID ON dbo.Transactions(CustomerID);
GO
CREATE INDEX idx_StockCode ON dbo.Transactions(StockCode);
GO

-- ============================================================================
-- Verify schema creation
-- ============================================================================
PRINT 'Schema created successfully!'
GO
