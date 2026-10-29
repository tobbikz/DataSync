-- Lab databases for DataSync (catalog) + DataLake (payload).
SELECT 'CREATE DATABASE datasync'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'datasync')\gexec

SELECT 'CREATE DATABASE "DataLake"'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'DataLake')\gexec
