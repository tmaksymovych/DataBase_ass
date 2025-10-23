drop database if exists p002;
create database p002;
use P002;
CREATE TABLE clients ( 
    customerId INT PRIMARY KEY AUTO_INCREMENT,
    first_name VARCHAR(50) NOT NULL,
    last_name VARCHAR(50) NOT NULL
);
CREATE TABLE clients_info (
    customerId INT NOT NULL,
    date_of_birth DATE,
    address VARCHAR(200),
    phone_number VARCHAR(12),
    email VARCHAR(80) UNIQUE,
    FOREIGN KEY (customerId) REFERENCES clients(customerId)
);
CREATE TABLE accounts (
    accountId INT PRIMARY KEY,
    customerId INT NOT NULL,
    balance DECIMAL(15, 2) DEFAULT 0.00,
    status VARCHAR(10) DEFAULT "Active" 
        CHECK (status in ("Active", "Closed")),
    open_date DATE NOT NULL,
    FOREIGN KEY (customerId) REFERENCES clients(customerId)
);