CREATE OR ALTER PROCEDURE silver.load_silver
AS
BEGIN

/* ============================================================
   SILVER LAYER ETL — WITH ERROR HANDLING & TIME TRACKING
   ============================================================ */

DECLARE @process_start DATETIME = GETDATE();
PRINT '============================================================';
PRINT '>> SILVER LAYER ETL STARTED AT: ' + CONVERT(VARCHAR, @process_start, 120);
PRINT '============================================================';


/* ============================================================
   STEP 1 — CRM CUSTOMER INFO
   ============================================================ */

DECLARE @start1 DATETIME = GETDATE();
PRINT '>> STEP 1 STARTED: CRM CUSTOMER INFO at ' + CONVERT(VARCHAR, @start1, 120);

BEGIN TRY

    TRUNCATE TABLE silver.crm_cust_info;

    WITH latest_record AS (
        SELECT *,
               ROW_NUMBER() OVER (
                    PARTITION BY cst_id
                    ORDER BY cst_create_date DESC
               ) AS flag_last
        FROM bronze.crm_cust_info
        WHERE cst_id IS NOT NULL
    )
    INSERT INTO silver.crm_cust_info (
        cst_id, cst_key, cst_firstname, cst_lastname,
        cst_marital_status, cst_gender, cst_create_date
    )
    SELECT
        cst_id,
        TRIM(cst_key),
        TRIM(cst_firstname),
        TRIM(cst_lastname),
        CASE 
            WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
            WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
            ELSE 'N/A'
        END,
        CASE 
            WHEN UPPER(TRIM(cst_gender)) = 'F' THEN 'Female'
            WHEN UPPER(TRIM(cst_gender)) = 'M' THEN 'Male'
            ELSE 'N/A'
        END,
        cst_create_date
    FROM latest_record
    WHERE flag_last = 1;

    PRINT '>> DONE: silver.crm_cust_info loaded successfully';

END TRY
BEGIN CATCH
    PRINT '>> ERROR IN STEP 1: ' + ERROR_MESSAGE();
END CATCH;

DECLARE @end1 DATETIME = GETDATE();
PRINT '>> STEP 1 DURATION (sec): ' + CAST(DATEDIFF(SECOND, @start1, @end1) AS VARCHAR);



/* ============================================================
   STEP 2 — CRM PRODUCT INFO
   ============================================================ */

DECLARE @start2 DATETIME = GETDATE();
PRINT '>> STEP 2 STARTED: CRM PRODUCT INFO at ' + CONVERT(VARCHAR, @start2, 120);

BEGIN TRY

    TRUNCATE TABLE silver.crm_prd_info;

    INSERT INTO silver.crm_prd_info (
        prd_id, cat_id, prd_key, prd_nm,
        prd_cost, prd_line, prd_start_dt, prd_end_dt
    )
    SELECT 
        prd_id,
        REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_'),
        SUBSTRING(prd_key, 7, LEN(prd_key)),
        TRIM(prd_nm),
        ISNULL(prd_cost, 0),
        CASE 
            WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'
            WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road'
            WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'Other Sales'
            WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
            ELSE 'N/A'
        END,
        CAST(prd_start_dt AS DATE),
        CASE 
            WHEN LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) IS NULL
            THEN prd_end_dt
            ELSE DATEADD(
                    DAY, -1,
                    LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)
                 )
        END
    FROM bronze.crm_prd_info;

    PRINT '>> DONE: silver.crm_prd_info loaded successfully';

END TRY
BEGIN CATCH
    PRINT '>> ERROR IN STEP 2: ' + ERROR_MESSAGE();
END CATCH;

DECLARE @end2 DATETIME = GETDATE();
PRINT '>> STEP 2 DURATION (sec): ' + CAST(DATEDIFF(SECOND, @start2, @end2) AS VARCHAR);



/* ============================================================
   STEP 3 — CRM SALES DETAILS
   ============================================================ */

DECLARE @start3 DATETIME = GETDATE();
PRINT '>> STEP 3 STARTED: CRM SALES DETAILS at ' + CONVERT(VARCHAR, @start3, 120);

BEGIN TRY

    TRUNCATE TABLE silver.crm_sales_details;

    INSERT INTO silver.crm_sales_details (
        sls_ord_num, sls_prd_key, sls_cust_id,
        sls_order_dt, sls_ship_dt, sls_due_dt,
        sls_sales, sls_quantity, sls_price
    )
    SELECT 
        TRIM(sls_ord_num),
        TRIM(sls_prd_key),
        sls_cust_id,

        CASE WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 
             THEN NULL ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE) END,

        CASE WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 
             THEN NULL ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE) END,

        CASE WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 
             THEN NULL ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE) END,

        CASE WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != ABS(sls_quantity) * sls_price
             THEN ABS(sls_quantity) * sls_price
             ELSE sls_sales END,

        sls_quantity,

        CASE WHEN sls_price IS NULL OR sls_price <= 0
             THEN sls_sales / NULLIF(sls_quantity, 0)
             ELSE sls_price END

    FROM bronze.crm_sales_details;

    PRINT '>> DONE: silver.crm_sales_details loaded successfully';

END TRY
BEGIN CATCH
    PRINT '>> ERROR IN STEP 3: ' + ERROR_MESSAGE();
END CATCH;

DECLARE @end3 DATETIME = GETDATE();
PRINT '>> STEP 3 DURATION (sec): ' + CAST(DATEDIFF(SECOND, @start3, @end3) AS VARCHAR);



/* ============================================================
   STEP 4 — ERP CUSTOMER
   ============================================================ */

DECLARE @start4 DATETIME = GETDATE();
PRINT '>> STEP 4 STARTED: ERP CUSTOMER at ' + CONVERT(VARCHAR, @start4, 120);

BEGIN TRY

    TRUNCATE TABLE silver.erp_cust_az12;

    INSERT INTO silver.erp_cust_az12 (cid, bdate, gen)
    SELECT 
        CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid)) ELSE cid END,
        CASE WHEN bdate > GETDATE() THEN NULL ELSE bdate END,
        CASE 
            WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
            WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
            ELSE 'N/A'
        END
    FROM bronze.erp_cust_az12;

    PRINT '>> DONE: silver.erp_cust_az12 loaded successfully';

END TRY
BEGIN CATCH
    PRINT '>> ERROR IN STEP 4: ' + ERROR_MESSAGE();
END CATCH;

DECLARE @end4 DATETIME = GETDATE();
PRINT '>> STEP 4 DURATION (sec): ' + CAST(DATEDIFF(SECOND, @start4, @end4) AS VARCHAR);



/* ============================================================
   STEP 5 — ERP LOCATION
   ============================================================ */

DECLARE @start5 DATETIME = GETDATE();
PRINT '>> STEP 5 STARTED: ERP LOCATION at ' + CONVERT(VARCHAR, @start5, 120);

BEGIN TRY

    TRUNCATE TABLE silver.erp_loc_a101;

    INSERT INTO silver.erp_loc_a101 (cid, cntry)
    SELECT 
        REPLACE(cid, '-', ''),
        CASE 
            WHEN TRIM(cntry) = 'DE' THEN 'Germany'
            WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
            WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'N/A'
            ELSE TRIM(cntry)
        END
    FROM bronze.erp_loc_a101;

    PRINT '>> DONE: silver.erp_loc_a101 loaded successfully';

END TRY
BEGIN CATCH
    PRINT '>> ERROR IN STEP 5: ' + ERROR_MESSAGE();
END CATCH;

DECLARE @end5 DATETIME = GETDATE();
PRINT '>> STEP 5 DURATION (sec): ' + CAST(DATEDIFF(SECOND, @start5, @end5) AS VARCHAR);



/* ============================================================
   STEP 6 — ERP PRODUCT CATEGORY
   ============================================================ */

DECLARE @start6 DATETIME = GETDATE();
PRINT '>> STEP 6 STARTED: ERP PRODUCT CATEGORY at ' + CONVERT(VARCHAR, @start6, 120);

BEGIN TRY

    TRUNCATE TABLE silver.erp_px_cat_g1v2;

    INSERT INTO silver.erp_px_cat_g1v2 (id, cat, subcat, maintenance)
    SELECT id, cat, subcat, maintenance
    FROM bronze.erp_px_cat_g1v2;

    PRINT '>> DONE: silver.erp_px_cat_g1v2 loaded successfully';

END TRY
BEGIN CATCH
    PRINT '>> ERROR IN STEP 6: ' + ERROR_MESSAGE();
END CATCH;

DECLARE @end6 DATETIME = GETDATE();
PRINT '>> STEP 6 DURATION (sec): ' + CAST(DATEDIFF(SECOND, @start6, @end6) AS VARCHAR);



/* ============================================================
   PROCESS END
   ============================================================ */

DECLARE @process_end DATETIME = GETDATE();
PRINT '============================================================';
PRINT '>> SILVER LAYER ETL FINISHED AT: ' + CONVERT(VARCHAR, @process_end, 120);
PRINT '>> TOTAL DURATION (sec): ' + CAST(DATEDIFF(SECOND, @process_start, @process_end) AS VARCHAR);
PRINT '============================================================';

END
GO
