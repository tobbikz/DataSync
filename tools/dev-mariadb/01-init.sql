-- Lab MariaDB CDC source (testdb + CDC-friendly grants).
CREATE DATABASE IF NOT EXISTS testdb;
CREATE USER IF NOT EXISTS 'datasync'@'%' IDENTIFIED BY 'DataSync_Dev1!';
GRANT ALL PRIVILEGES ON testdb.* TO 'datasync'@'%';
GRANT REPLICATION SLAVE, REPLICATION CLIENT, BINLOG MONITOR ON *.* TO 'datasync'@'%';
GRANT SELECT, SHOW VIEW ON *.* TO 'datasync'@'%';
FLUSH PRIVILEGES;

USE testdb;

CREATE TABLE IF NOT EXISTS customers (
  id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  email VARCHAR(255) NOT NULL,
  name VARCHAR(255) NOT NULL,
  active TINYINT(1) NOT NULL DEFAULT 1,
  UNIQUE KEY uk_customers_email (email)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS orders_probe (
  id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  customer_id BIGINT NOT NULL,
  sku VARCHAR(64) NOT NULL,
  qty INT NOT NULL DEFAULT 1,
  KEY idx_orders_customer (customer_id)
) ENGINE=InnoDB;

INSERT INTO customers (email, name, active)
SELECT 'seed@lab.local', 'Lab Seed', 1
WHERE NOT EXISTS (SELECT 1 FROM customers WHERE email = 'seed@lab.local');
