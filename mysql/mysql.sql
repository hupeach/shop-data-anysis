CREATE database IF NOT EXISTS echarts;
ALTER DATABASE echarts CHARACTER SET utf8 COLLATE utf8_general_ci;
use echarts;

-- AnnualTrend 表
DROP TABLE IF EXISTS annual_trend;
CREATE TABLE annual_trend (
    order_date_year INT,
    order_quantity INT,
    sales_amount DECIMAL(10, 2),
    profit DECIMAL(10, 2),
    order_growth_rate DECIMAL(10, 2),
    sales_growth_rate DECIMAL(10, 2),
    profit_growth_rate DECIMAL(10, 2)
);

-- ReturnAnalysis 表
DROP TABLE IF EXISTS return_analysis;
CREATE TABLE return_analysis (
    order_date_year INT,
    return_quantity INT,
    total_order_quantity INT,
    return_rate DECIMAL(10, 2)
);

-- MonthlySales 表
DROP TABLE IF EXISTS monthly_sales;
CREATE TABLE monthly_sales (
    order_date_year INT,
    order_date_month INT,
    total_sales DECIMAL(10, 2)
);

-- RegionAnalysis 表
DROP TABLE IF EXISTS region_analysis;
CREATE TABLE region_analysis (
    region VARCHAR(255),
    sales_amount_sum DECIMAL(10, 2),
    profit_sum DECIMAL(10, 2)
);

-- ProvSales 表
DROP TABLE IF EXISTS prov_sales;
CREATE TABLE prov_sales (
    province VARCHAR(255),
    sales_amount_sum DECIMAL(10, 2),
    profit_sum DECIMAL(10, 2)
);

-- ProductAnalysis 表
DROP TABLE IF EXISTS product_analysis;
CREATE TABLE product_analysis (
    category VARCHAR(255),
    subcategory VARCHAR(255),
    sales_amount_sum DECIMAL(10, 2),
    profit_sum DECIMAL(10, 2),
    average_discount_rate DECIMAL(10, 2),
    order_quantity INT
);

-- ClientAnalysis 表
DROP TABLE IF EXISTS client_analysis;
CREATE TABLE client_analysis (
    segment VARCHAR(255),
    order_quantity INT,
    customer_count INT
);

DROP TABLE IF EXISTS product_return_analysis;
CREATE TABLE product_return_analysis (
    subcategory VARCHAR(255),
    return_quantity INT,
    return_rate DECIMAL(10, 2)
);
