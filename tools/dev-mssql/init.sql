-- Dev MSSQL schema + CDC for DataSync (testdb.dbo.customers, orders_probe)
IF NOT EXISTS (SELECT 1 FROM sys.databases WHERE name = N'testdb')
BEGIN
    CREATE DATABASE testdb;
END
GO

USE testdb;
GO

IF OBJECT_ID(N'dbo.customers', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.customers (
        id INT NOT NULL PRIMARY KEY IDENTITY(1,1),
        name NVARCHAR(200) NOT NULL,
        active BIT NOT NULL CONSTRAINT DF_customers_active DEFAULT (1),
        updated_at DATETIME2(3) NOT NULL CONSTRAINT DF_customers_updated DEFAULT SYSUTCDATETIME()
    );
END
GO

IF OBJECT_ID(N'dbo.orders_probe', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.orders_probe (
        id INT NOT NULL PRIMARY KEY IDENTITY(1,1),
        sku NVARCHAR(64) NOT NULL,
        qty INT NOT NULL DEFAULT 1,
        created_at DATETIME2(3) NOT NULL CONSTRAINT DF_orders_created DEFAULT SYSUTCDATETIME()
    );
END
GO

IF NOT EXISTS (SELECT 1 FROM dbo.customers)
BEGIN
    INSERT INTO dbo.customers (name, active) VALUES (N'Dev Customer', 1), (N'Dev Customer 2', 0);
END
GO

IF NOT EXISTS (SELECT 1 FROM dbo.orders_probe)
BEGIN
    INSERT INTO dbo.orders_probe (sku, qty) VALUES (N'SMOKE-SKU', 2);
END
GO

IF (SELECT is_cdc_enabled FROM sys.databases WHERE name = DB_NAME()) = 0
BEGIN
    EXEC sys.sp_cdc_enable_db;
END
GO

IF NOT EXISTS (
    SELECT 1 FROM cdc.change_tables ct
    INNER JOIN sys.tables t ON ct.source_object_id = t.object_id
    INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
    WHERE s.name = N'dbo' AND t.name = N'customers'
)
BEGIN
    EXEC sys.sp_cdc_enable_table
        @source_schema = N'dbo',
        @source_name = N'customers',
        @role_name = NULL,
        @supports_net_changes = 1;
END
GO

IF NOT EXISTS (
    SELECT 1 FROM cdc.change_tables ct
    INNER JOIN sys.tables t ON ct.source_object_id = t.object_id
    INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
    WHERE s.name = N'dbo' AND t.name = N'orders_probe'
)
BEGIN
    EXEC sys.sp_cdc_enable_table
        @source_schema = N'dbo',
        @source_name = N'orders_probe',
        @role_name = NULL,
        @supports_net_changes = 1;
END
GO
