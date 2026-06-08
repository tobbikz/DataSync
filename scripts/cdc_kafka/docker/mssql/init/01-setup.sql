-- Idempotent local dev setup: login, CDC database, sample table.
SET NOCOUNT ON;

IF NOT EXISTS (SELECT 1 FROM sys.server_principals WHERE name = N'tomy.berrios')
BEGIN
    CREATE LOGIN [tomy.berrios] WITH PASSWORD = N'Yucaquemada1', CHECK_POLICY = OFF;
END
GO

IF NOT EXISTS (SELECT 1 FROM sys.databases WHERE name = N'datalake_cdc')
BEGIN
    CREATE DATABASE datalake_cdc;
END
GO

USE datalake_cdc;
GO

IF NOT EXISTS (SELECT 1 FROM sys.database_principals WHERE name = N'tomy.berrios')
BEGIN
    CREATE USER [tomy.berrios] FOR LOGIN [tomy.berrios];
END
GO

IF NOT EXISTS (
    SELECT 1
    FROM sys.database_role_members drm
    INNER JOIN sys.database_principals r ON drm.role_principal_id = r.principal_id
    INNER JOIN sys.database_principals m ON drm.member_principal_id = m.principal_id
    WHERE r.name = N'db_owner' AND m.name = N'tomy.berrios'
)
BEGIN
    ALTER ROLE db_owner ADD MEMBER [tomy.berrios];
END
GO

IF (SELECT is_cdc_enabled FROM sys.databases WHERE name = DB_NAME()) = 0
BEGIN
    EXEC sys.sp_cdc_enable_db;
END
GO

IF OBJECT_ID(N'dbo.countries', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.countries (
        country_id INT NOT NULL PRIMARY KEY,
        name NVARCHAR(100) NOT NULL,
        updated_at DATETIME2(3) NOT NULL DEFAULT SYSUTCDATETIME()
    );
    INSERT INTO dbo.countries (country_id, name) VALUES (1, N'Testland'), (2, N'Exampleia');
END
GO

IF NOT EXISTS (
    SELECT 1
    FROM cdc.change_tables ct
    INNER JOIN sys.tables t ON ct.source_object_id = t.object_id
    WHERE t.name = N'countries' AND SCHEMA_NAME(t.schema_id) = N'dbo'
)
BEGIN
    EXEC sys.sp_cdc_enable_table
        @source_schema = N'dbo',
        @source_name = N'countries',
        @role_name = NULL,
        @supports_net_changes = 1;
END
GO

PRINT N'datalake_cdc CDC ready';
GO
