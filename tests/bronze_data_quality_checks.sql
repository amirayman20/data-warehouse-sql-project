/* ============================================================
   DATA QUALITY CHECKS — BRONZE & SILVER LAYER
   ============================================================ */

Script Purpose:
    This script performs a comprehensive set of Data Quality Checks
    on the Bronze Layer before loading the Silver Layer.

    These checks ensure:
        - No duplicate or missing primary keys
        - No unwanted spaces in string fields
        - Standardized and consistent categorical values
        - Valid and logical date ranges
        - Correct business rules (e.g., sales = quantity * price)
        - Clean and reliable data for Silver ETL

When to Run:
    - Run this script BEFORE executing:
          EXEC silver.load_silver;
    - Use it to detect anomalies, inconsistencies, or invalid records
      that may affect the Silver Layer.

Notes:
    - This script does NOT modify data.
    - It only reports issues for investigation.
    - All fixes should be applied in the Bronze Layer.

===========================================================
*/
/* ============================================================
   SECTION 1 — CRM PRODUCT CHECKS
   ============================================================ */

-------------------------------------------------------------
-- CHECK 1: Duplicate or Missing Product Keys
-------------------------------------------------------------
SELECT 
    prd_key,
    COUNT(*) AS occurrences
FROM bronze.crm_prd_info
GROUP BY prd_key
HAVING COUNT(*) > 1 OR prd_key IS NULL;


-------------------------------------------------------------
-- CHECK 2: Product Names with Leading/Trailing Spaces
-------------------------------------------------------------
SELECT 
    prd_nm
FROM bronze.crm_prd_info
WHERE prd_nm != TRIM(prd_nm);


-------------------------------------------------------------
-- CHECK 3: Invalid or Missing Product Cost
-------------------------------------------------------------
SELECT 
    prd_cost
FROM bronze.crm_prd_info
WHERE prd_cost < 0 OR prd_cost IS NULL;


-------------------------------------------------------------
-- CHECK 4: Distinct Product Lines (Detect Unexpected Values)
-------------------------------------------------------------
SELECT DISTINCT 
    prd_line
FROM silver.crm_prd_info;


-------------------------------------------------------------
-- CHECK 5: Invalid Product Date Ranges
-- (Start Date > End Date)
-------------------------------------------------------------
SELECT 
    *
FROM silver.crm_prd_info
WHERE prd_start_dt > prd_end_dt;



/* ============================================================
   SECTION 2 — CRM SALES DETAILS CHECKS
   ============================================================ */

-------------------------------------------------------------
-- CHECK 6: Invalid Due Dates
-- (0, wrong length, out of valid range)
-------------------------------------------------------------
SELECT 
    NULLIF(sls_due_dt, 0) AS sls_due_dt
FROM bronze.crm_sales_details
WHERE sls_due_dt <= 0
   OR LEN(sls_due_dt) != 8
   OR sls_due_dt > 20500101
   OR sls_due_dt < 19000101;


-------------------------------------------------------------
-- CHECK 7: Invalid Order Date Logic
-- (Order > Ship OR Order > Due)
-------------------------------------------------------------
SELECT *
FROM bronze.crm_sales_details
WHERE sls_order_dt > sls_ship_dt
   OR sls_order_dt > sls_due_dt;


-------------------------------------------------------------
-- CHECK 8: Sales Amount Consistency
-- sls_sales = quantity * price
-- Detect invalid or inconsistent values
-------------------------------------------------------------
SELECT DISTINCT
    sls_quantity,
    sls_price AS old_price,
    sls_sales AS old_sales,
    CASE 
        WHEN sls_sales IS NULL 
          OR sls_sales <= 0 
          OR sls_sales != ABS(sls_quantity) * sls_price
        THEN ABS(sls_quantity) * sls_price
        ELSE sls_sales
    END AS corrected_sales,
    CASE 
        WHEN sls_price IS NULL 
          OR sls_price <= 0
        THEN sls_sales / NULLIF(sls_quantity, 0)
        ELSE sls_price
    END AS corrected_price
FROM bronze.crm_sales_details
WHERE sls_sales != sls_price * sls_quantity
   OR sls_sales IS NULL 
   OR sls_price IS NULL 
   OR sls_quantity IS NULL
   OR sls_sales <= 0 
   OR sls_price <= 0 
   OR sls_quantity <= 0
ORDER BY old_sales, sls_quantity, sls_price;


-------------------------------------------------------------
-- CHECK 9: Silver Sales Table Review
-------------------------------------------------------------
SELECT *
FROM silver.crm_sales_details;



/* ============================================================
   SECTION 3 — ERP CUSTOMER CHECKS
   ============================================================ */

-------------------------------------------------------------
-- CHECK 10: Customer ID, Birthdate, Gender Cleanup Logic
-------------------------------------------------------------
SELECT 
    CASE 
        WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
        ELSE cid
    END AS cleaned_cid,
    CASE 
        WHEN bdate > GETDATE() THEN NULL 
        ELSE bdate 
    END AS cleaned_bdate,
    CASE 
        WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
        WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
        ELSE 'N/A'
    END AS cleaned_gender
FROM bronze.erp_cust_az12;



/* ============================================================
   SECTION 4 — ERP LOCATION CHECKS
   ============================================================ */

-------------------------------------------------------------
-- CHECK 11: Country Cleanup Logic
-------------------------------------------------------------
SELECT 
    REPLACE(cid, '-', '') AS cleaned_cid,
    CASE 
        WHEN TRIM(cntry) = 'DE' THEN 'Germany'
        WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
        WHEN TRIM(cntry) = '' OR TRIM(cntry) IS NULL THEN 'N/A'
        ELSE TRIM(cntry)
    END AS cleaned_cntry
FROM bronze.erp_loc_a101;



/* ============================================================
   SECTION 5 — ERP PRODUCT CATEGORY CHECKS
   ============================================================ */

-------------------------------------------------------------
-- CHECK 12: Product Category Review
-------------------------------------------------------------
SELECT 
    id,
    cat,
    subcat,
    maintenance
FROM bronze.erp_px_cat_g1v2;
