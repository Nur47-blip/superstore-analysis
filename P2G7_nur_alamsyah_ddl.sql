-- Membuat Database
CREATE DATABASE gc7;

-- Membuat Tabel
CREATE TABLE table_gc7 (
    "Row ID" BIGINT PRIMARY KEY,
    "Order ID" VARCHAR(255),
    "Order Date" DATE,
    "Ship Date" DATE,
    "Ship Mode" VARCHAR(255),
    "Customer ID" VARCHAR(255),
    "Customer Name" VARCHAR(255),
    "Segment" VARCHAR(255),
    "Country" VARCHAR(255),
    "City" VARCHAR(255),
    "State" VARCHAR(255),
    "Postal Code" BIGINT,
    "Region" VARCHAR(255),
    "Product ID" VARCHAR(255),
    "Category" VARCHAR(255),
    "Sub-Category" VARCHAR(255),
    "Product Name" VARCHAR(255),
    "Sales" DOUBLE PRECISION,
    "Quantity" BIGINT,
    "Discount" DOUBLE PRECISION,
    "Profit" DOUBLE PRECISION
);

-- Menggunakan PSQL untuk memasukkan data ke postgreSQL
psql -h localhost -U postgres -d gc7

SET datestyle = 'MDY';
\COPY table_gc7("Row ID", "Order ID", "Order Date", "Ship Date", "Ship Mode", "Customer ID", "Customer Name", "Segment", "Country", "City", "State", "Postal Code", "Region", "Product ID", "Category", "Sub-Category", "Product Name", "Sales", "Quantity", "Discount", "Profit")  FROM 'C:\Users\LENOVO\github-classroom\FTDS-assignment-bay\p2-ftds023-rmt-g7-Nur47-blip\P2G7_nur_alamsyah_data_raw.csv' DELIMITER ',' CSV HEADER ENCODING 'UTF8';