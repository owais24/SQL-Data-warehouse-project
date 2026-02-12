/*
===============================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
===============================================================================
Script Purpose:
    This stored procedure loads data into the 'bronze' schema from external CSV files. 
    It performs the following actions:
    - Truncates the bronze tables before loading data.
    - Uses the `BULK INSERT` command to load data from csv Files to bronze tables.

Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC bronze.load_bronze;
===============================================================================
*/


CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
    DECLARE @Start_time DATETIME ,@End_time DATETIME, @start_batch_time DATETIME, @end_batch_time DATETIME ;
    BEGIN TRY
         SET @start_batch_time= GETDATE()
        print '===========================================';
        print 'LOADING BRONZE LAYER';
        PRINT '===========================================';

        PRINT '-------------------------------------------';
        PRINT 'LOADING CRM TABLES'
        PRINT '-------------------------------------------';

        SET @Start_time= GETDATE()
        PRINT '>> Truncating Table: bronze.crm_cust_info';
        TRUNCATE TABLE bronze.crm_cust_info
        PRINT '>> Inserting Data Into: bronze.crm_cust_info';
        BULK INSERT bronze.crm_cust_info
        FROM 'the target source files'
        WITH (
          FIRSTROW=2,
          FIELDTERMINATOR=',',
          TABLOCK
        );
         SET @End_time= GETDATE()
         Print '>> Load Time :' +  CAST(DATEDIFF(second, @Start_time, @End_time) AS NVARCHAR) + 'seconds'
         print '---------------'
        --select COUNT(*)from bronze.crm_cust_info;

        --------------------------------------------------------------------------------------------------
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: bronze.crm_prd_info';
        TRUNCATE TABLE bronze.crm_prd_info
        PRINT '>> Inserting Data Into: bronze.crm_prd_info';
        BULK INSERT bronze.crm_prd_info
        FROM 'the target source files'
        WITH (
           FIRSTROW=2,
           FIELDTERMINATOR=',',
           TABLOCK
        );
        SET @End_time= GETDATE()
        Print '>> Load Time :' +  CAST(DATEDIFF(second, @Start_time, @End_time) AS NVARCHAR) + 'seconds'
        print '---------------'
        --select * from bronze.crm_prd_info;

        ----------------------------------------------------------------------------------------------------
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: bronze.crm_sales_details';
        TRUNCATE TABLE bronze.crm_sales_details
        PRINT '>> Inserting Data Into: bronze.crm_sales_details';
        BULK INSERT bronze.crm_sales_details
         FROM 'the target source files'
        WITH (
            FIRSTROW= 2,
            FIELDTERMINATOR= ',',
            TABLOCK
        )
        SET @End_time= GETDATE()
        Print '>> Load Time :' +  CAST(DATEDIFF(second, @Start_time, @End_time) AS NVARCHAR) + 'seconds'
        print '---------------'
        -- select COUNT(*) from bronze.crm_sales_details;
        ------------------------------------------------------------------------------------------------------
        SET @start_time = GETDATE();
        PRINT '-------------------------------------------';
        PRINT 'LOADING ERP TABLES'
        PRINT '-------------------------------------------';

        PRINT '>> Truncating Table: bronze.erp_cust_az12';
        TRUNCATE TABLE bronze.erp_cust_az12
        PRINT '>> Inserting Data Into: bronze.erp_cust_az12';
        BULK INSERT bronze.erp_cust_az12
        FROM 'the target source files'
        WITH (
             FIRSTROW=2,
             FIELDTERMINATOR=',',
             TABLOCK
        )
        SET @End_time= GETDATE()
        Print '>> Load Time :' +  CAST(DATEDIFF(second, @Start_time, @End_time) AS NVARCHAR) + 'seconds'
        print '---------------'
        --select * FROM bronze.erp_cust_az12;
        ---------------------------------------------------------------------------------------------------------
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: bronze.erp_loc_a101';
        TRUNCATE TABLE bronze.erp_loc_a101
        PRINT '>> Inserting Data Into: bronze.erp_loc_a101';
        BULK INSERT bronze.erp_loc_a101
         FROM 'the target source files'
        WITH (
             FIRSTROW=2,
             FIELDTERMINATOR=',',
             TABLOCK
        )
        SET @End_time= GETDATE()
        Print '>> Load Time :' +  CAST(DATEDIFF(second, @Start_time, @End_time) AS NVARCHAR) + 'seconds'
        print '---------------'
        --select  * FROM bronze.erp_loc_a101;
        ----------------------------------------------------------------------------------------------------------
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: bronze.erp_px_cat_g1v2';
        TRUNCATE TABLE bronze.erp_px_cat_g1v2
        PRINT '>> Inserting Data Into: bronze.erp_px_cat_g1v2';
        BULK INSERT bronze.erp_px_cat_g1v2
        FROM 'the target source files'
        WITH (
             FIRSTROW=2,
             FIELDTERMINATOR=',',
             TABLOCK
        );
        SET @End_time= GETDATE()
        Print '>> Load Time :' +  CAST(DATEDIFF(second, @Start_time, @End_time) AS NVARCHAR) + 'seconds'
        print '---------------'

    --select  * FROM bronze.erp_px_cat_g1v2;
        SET @end_batch_time = GETDATE()
        PRINT '=========================================='
		PRINT 'Loading Bronze Layer is Completed';
        PRINT '   - Total Load Duration: ' + CAST(DATEDIFF(SECOND, @start_batch_time, @end_batch_time) AS NVARCHAR) + ' seconds';
		PRINT '=========================================='
    END TRY
    BEGIN CATCH
    print'============================================'
    print'ERROR OCCURED  DURING  LOADING  BRONZE LAYER'
    PRINT 'ERROR MESSAGE' + ERROR_MESSAGE();
    PRINT 'ERROR MESSAGE' + CAST(ERROR_NUMBER()AS NVARCHAR);
    PRINT 'ERROR MESSAGE' + CAST(ERROR_STATE() AS NVARCHAR);
    PRINT 'Error Line: '  + CAST(ERROR_LINE()   AS NVARCHAR(10));
    print'============================================'
    END CATCH 

END;
