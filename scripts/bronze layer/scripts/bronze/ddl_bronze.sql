/*
=============================================================
DDL Script: Create Bronze Tables
=============================================================
Purpose:
    Defines the DDL structure for all Bronze Layer tables.
    Drops existing tables (if exist) and recreates them cleanly.

Usage:
    Run this script before loading Bronze data.
=============================================================
*/

-------------------------------------------------------------
-- CRM: Customer Info
-------------------------------------------------------------
IF OBJECT_ID('bronze.crm_cust_info', 'U') IS NOT NULL
    DROP TABLE bronze.crm_cust_info;
GO

CREATE TABLE bronze.crm_cust_info (
    cst_id INT,
    cst_key VARCHAR(50),
    cst_firstname VARCHAR(50),
    cst_lastname VARCHAR(50),
    cst_material_status VARCHAR(50),
    cst_gender VARCHAR(50),
    cst_create_date DATE
);
GO


-------------------------------------------------------------
-- CRM: Product Info
-------------------------------------------------------------
IF OBJECT_ID('bronze.crm_prd_info', 'U') IS NOT NULL
    DROP TABLE bronze.crm_prd_info;
GO

CREATE TABLE bronze.crm_prd_info (
    prd_id INT,
    prd_key VARCHAR(50),
    prd_nm VARCHAR(50),
    prd_cost INT,
    prd_line VARCHAR(50),
    prd_start_dt DATETIME,
    prd_end_dt DATETIME
);
GO


-------------------------------------------------------------
-- CRM: Sales Details
-------------------------------------------------------------
IF OBJECT_ID('bronze.crm_sales_details', 'U') IS NOT NULL
    DROP TABLE bronze.crm_sales_details;
GO

CREATE TABLE bronze.crm_sales_details (
    sls_ord_num VARCHAR(50),
    sls_prd_key VARCHAR(50),
    sls_cust_id INT,
    sls_order_dt DATE,
    sls_ship_dt DATE,
    sls_due_dt DATE,
    sls_sales INT,
    sls_quantity INT,
    sls_price INT
);
GO


-------------------------------------------------------------
-- ERP: Location A101
-------------------------------------------------------------
IF OBJECT_ID('bronze.erp_loc_a101', 'U') IS NOT NULL
    DROP TABLE bronze.erp_loc_a101;
GO

CREATE TABLE bronze.erp_loc_a101 (
    cid VARCHAR(50),
    cntry VARCHAR(50)
);
GO


-------------------------------------------------------------
-- ERP: Customer AZ12
-------------------------------------------------------------
IF OBJECT_ID('bronze.erp_cust_az12', 'U') IS NOT NULL
    DROP TABLE bronze.erp_cust_az12;
GO

CREATE TABLE bronze.erp_cust_az12 (
    cid VARCHAR(50),
    bdate DATE,
    gen VARCHAR(50)
);
GO


-------------------------------------------------------------
-- ERP: Product Category G1V2
-------------------------------------------------------------
IF OBJECT_ID('bronze.erp_px_cat_g1v2', 'U') IS NOT NULL
    DROP TABLE bronze.erp_px_cat_g1v2;
GO

CREATE TABLE bronze.erp_px_cat_g1v2 (
    id VARCHAR(50),
    cat VARCHAR(50),
    subcat VARCHAR(50),
    maintenance VARCHAR(50)
);
GO
