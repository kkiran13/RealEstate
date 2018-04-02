CREATE TABLE IF NOT EXISTS transactions (
id INTEGER NOT NULL,
property_address VARCHAR(5000) NULL,
buyer VARCHAR(100) NULL,
seller VARCHAR(100) NULL,
transaction_date DATE NULL,
property_id INTEGER NOT NULL,
property_type VARCHAR(10) NULL,
transaction_amount DECIMAL(20,1) NULL,
loan_amount DECIMAL(20,1) NULL,
lender VARCHAR(100) NULL,
sqft INTEGER NULL,
year_built INTEGER NULL,
PRIMARY KEY (id)
);
-- SORTKEY(property_id)
