DROP TABLE IF EXISTS customers;
DROP TABLE IF EXISTS emails;
DROP TABLE IF EXISTS phones;

CREATE TABLE customers (
fiscal_id VARCHAR PRIMARY KEY, 
first_name VARCHAR, 
last_name VARCHAR, 
gender VARCHAR, 
birth_date DATETIME, 
age INTEGER, 
age_group INTEGER, 
due_date DATETIME, 
delinquency INTEGER, 
due_balance INTEGER, 
address VARCHAR, 
ocupation VARCHAR, 
best_contact_ocupation BOOLEAN 
);

CREATE TABLE emails (
fiscal_id VARCHAR, 
email VARCHAR, 
status VARCHAR, 
priority INTEGER, 
PRIMARY KEY(fiscal_id, email)
);

CREATE TABLE phones (
fiscal_id VARCHAR, 
phone VARCHAR, 
status VARCHAR, 
priority INTEGER, 
PRIMARY KEY(fiscal_id, phone)
);