/*
=============================================================
Stored Procedure: bronze.load_bronze
=============================================================
Purpose:
    Loads raw CRM & ERP data into Bronze Layer tables.
    Includes:
        - Truncate + Bulk Insert for all Bronze tables
        - Load duration tracking per table
        - Batch duration tracking
        - TRY/CATCH error handling

Usage:
    EXEC bronze.load_bronze;

Notes:
    - Ensure CSV files exist in the specified paths.
    - Update file paths if moved.
=============================================================
*/

CREATE OR ALTER PROCEDURE bronze.load_bronze
AS
BEGIN
    DECLARE @START_TIME DATETIME, 
            @END_TIME DATETIME, 
            @BATCH_START_TIME DATETIME, 
            @BATCH_END_TIME DATETIME;

    BEGIN TRY
        
        ------------------------------------------------------
        -- Start Batch Timer
        ------------------------------------------------------
        SET @BATCH_START_TIME = GETDATE();

        PRINT '====================================================';
        PRINT '>> Starting Bronze Layer Data Loading';
        PRINT '====================================================';


        ------------------------------------------------------
        -- CRM SECTION
        ------------------------------------------------------
        PRINT '----------------------------------------------------';
        PRINT '>> Loading CRM Source Tables...';
        PRINT '----------------------------------------------------';


        ------------------------------------------------------
        -- CRM Customer Info
        ------------------------------------------------------
        PRINT '-> Loading CRM Customer Info...';
        SET @START_TIME = GETDATE();

        TRUNCATE TABLE bronze.crm_cust_info;

        BULK INSERT bronze.crm_cust_info
        FROM 'F:\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
        WITH (FIRSTROW = 2, FIELDTERMINATOR = ',', TABLOCK);

        SET @END_TIME = GETDATE();
        PRINT '>> LOAD DURATION: ' + CAST(DATEDIFF(SECOND, @START_TIME, @END_TIME) AS NVARCHAR(10)) + ' SECONDS';
        PRINT '-- CRM Customer Info Loaded Successfully';
        PRINT '----------------------------------------------------';


        ------------------------------------------------------
        -- CRM Product Info
        ------------------------------------------------------
        PRINT '-> Loading CRM Product Info...';
        SET @START_TIME = GETDATE();

        TRUNCATE TABLE bronze.crm_prd_info;

        BULK INSERT bronze.crm_prd_info
        FROM 'F:\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
        WITH (FIRSTROW = 2, FIELDTERMINATOR = ',', TABLOCK);

        SET @END_TIME = GETDATE();
        PRINT '>> LOAD DURATION: ' + CAST(DATEDIFF(SECOND, @START_TIME, @END_TIME) AS NVARCHAR(10)) + ' SECONDS';
        PRINT '-- CRM Product Info Loaded Successfully';
        PRINT '----------------------------------------------------';


        ------------------------------------------------------
        -- CRM Sales Details
        ------------------------------------------------------
        PRINT '-> Loading CRM Sales Details...';
        SET @START_TIME = GETDATE();

        TRUNCATE TABLE bronze.crm_sales_details;

        BULK INSERT bronze.crm_sales_details
        FROM 'F:\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
        WITH (FIRSTROW = 2, FIELDTERMINATOR = ',', TABLOCK);

        SET @END_TIME = GETDATE();
        PRINT '>> LOAD DURATION: ' + CAST(DATEDIFF(SECOND, @START_TIME, @END_TIME) AS NVARCHAR(10)) + ' SECONDS';
        PRINT '-- CRM Sales Details Loaded Successfully';
        PRINT '----------------------------------------------------';


        ------------------------------------------------------
        -- ERP SECTION
        ------------------------------------------------------
        PRINT '----------------------------------------------------';
        PRINT '>> Loading ERP Source Tables...';
        PRINT '----------------------------------------------------';


        ------------------------------------------------------
        -- ERP Customer AZ12
        ------------------------------------------------------
        PRINT '-> Loading ERP Customer AZ12...';
        SET @START_TIME = GETDATE();

        TRUNCATE TABLE bronze.erp_cust_az12;

        BULK INSERT bronze.erp_cust_az12
        FROM 'F:\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
        WITH (FIRSTROW = 2, FIELDTERMINATOR = ',', TABLOCK);

        SET @END_TIME = GETDATE();
        PRINT '>> LOAD DURATION: ' + CAST(DATEDIFF(SECOND, @START_TIME, @END_TIME) AS NVARCHAR(10)) + ' SECONDS';
        PRINT '-- ERP Customer AZ12 Loaded Successfully';
        PRINT '----------------------------------------------------';


        ------------------------------------------------------
        -- ERP Location A101
        ------------------------------------------------------
        PRINT '-> Loading ERP Location A101...';
        SET @START_TIME = GETDATE();

        TRUNCATE TABLE bronze.erp_loc_a101;

        BULK INSERT bronze.erp_loc_a101
        FROM 'F:\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
        WITH (FIRSTROW = 2, FIELDTERMINATOR = ',', TABLOCK);

        SET @END_TIME = GETDATE();
        PRINT '>> LOAD DURATION: ' + CAST(DATEDIFF(SECOND, @START_TIME, @END_TIME) AS NVARCHAR(10)) + ' SECONDS';
        PRINT '-- ERP Location A101 Loaded Successfully';
        PRINT '----------------------------------------------------';


        ------------------------------------------------------
        -- ERP Product Category G1V2
        ------------------------------------------------------
        PRINT '-> Loading ERP Product Category G1V2...';
        SET @START_TIME = GETDATE();

        TRUNCATE TABLE bronze.erp_px_cat_g1v2;

        BULK INSERT bronze.erp_px_cat_g1v2
        FROM 'F:\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
        WITH (FIRSTROW = 2, FIELDTERMINATOR = ',', TABLOCK);

        SET @END_TIME = GETDATE();
        PRINT '>> LOAD DURATION: ' + CAST(DATEDIFF(SECOND, @START_TIME, @END_TIME) AS NVARCHAR(10)) + ' SECONDS';
        PRINT '-- ERP Product Category G1V2 Loaded Successfully';
        PRINT '----------------------------------------------------';


        ------------------------------------------------------
        -- End Batch
        ------------------------------------------------------
        SET @BATCH_END_TIME = GETDATE();
        PRINT '----------------------------------------';
        PRINT 'LOADING BRONZE LAYER COMPLETED';
        PRINT '>> TOTAL LOAD DURATION: ' + CAST(DATEDIFF(SECOND, @BATCH_START_TIME, @BATCH_END_TIME) AS NVARCHAR(10)) + ' SECONDS';
        PRINT '----------------------------------------';

    END TRY


    BEGIN CATCH
        PRINT '=======================================';
        PRINT 'ERROR OCCURRED DURING LOADING BRONZE LAYER';
        PRINT 'ERROR MESSAGE: ' + ERROR_MESSAGE();
        PRINT 'ERROR NUMBER: ' + CAST(ERROR_NUMBER() AS NVARCHAR(10));
        PRINT 'ERROR STATE: ' + CAST(ERROR_STATE() AS NVARCHAR(10));
        PRINT '=======================================';
    END CATCH

END;
