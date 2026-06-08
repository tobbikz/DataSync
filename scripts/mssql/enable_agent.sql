-- Enable SQL Server Agent XPs (Agent service must run — MSSQL_AGENT_ENABLED=true in Docker).
SET NOCOUNT ON;

EXEC sp_configure 'show advanced options', 1;
RECONFIGURE;
EXEC sp_configure 'Agent XPs', 1;
RECONFIGURE;

SELECT servicename, status_desc
FROM sys.dm_server_services
WHERE servicename LIKE N'%Agent%';
