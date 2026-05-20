/*
===============================================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
===============================================================================
Script Purpose:
    This stored procedure performs the ETL (Extract, Transform, Load) process to 
    populate the 'silver' schema tables from the 'bronze' schema.
	Actions Performed:
		- Truncates Silver tables.
		- Inserts transformed and cleansed data from Bronze into Silver tables.
		
Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC Silver.load_silver;
===============================================================================
*/

EXEC Silver.load_silver
---CONSISTENCY
-- IF YOU INTRODUCE AN IMPROVEMENT, LIKE BETTER LOGGING OR ERROR HANDLING,IN ONE STORED PROCEDURE,APPLY IT TO OTHERS 
--TO MAINTAIN CONSISTENT STANDARDS AND BENEFITS.

CREATE OR ALTER PROCEDURE Silver.load_silver AS 
BEGIN
     DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME; 
     BEGIN TRY
        SET @batch_start_time = GETDATE();
        PRINT '================================================';
        PRINT 'Loading Silver Layer';
        PRINT '================================================';

		PRINT '------------------------------------------------';
		PRINT 'Loading CRM Tables';
		PRINT '------------------------------------------------';
    -- Loading silver.crm_cust_info
        SET @start_time = GETDATE();
    PRINT'>>Truncating Table Silver.crm_cust_info'
    TRUNCATE TABLE Silver.crm_cust_info;
    Print'>>Inserting Data Into:Silver.crm_cust_info'
    SELECT * FROM Silver.crm_cust_info
    INSERT INTO Silver.crm_cust_info (
       cst_id,
       cst_key,
       cst_firstname,
       cst_lastname,
       cst_marital_status,
       cst_gndr,
       cst_create_date
    )
    SELECT 
    cst_id,
    cst_key,
    TRIM(cst_firstname) AS cst_firstname,
    TRIM(cst_lastname) AS cst_lastname,
    CASE WHEN UPPER(TRIM(cst_material_status))='S' THEN 'Single'           -- HERE WE APPLY UPPER() JUST IN CASE MIXED-CASE VALUES APPEAR LATER IN YOUR COLUMN
         WHEN UPPER(TRIM(cst_material_status))='M' THEN 'Married'          --HERE WE APPLY TRIM()JUST IN CASE SPACES APPEAR LATER IN YOUR COLUMN
         ELSE 'N/A'
     END cst_material_status,
    CASE WHEN UPPER(TRIM(cst_gndr))= 'F' THEN 'Female'       --HERE WE APPLY UPPER() JUST IN CASE MIXED-CASE VALUES APPEAR LATER IN YOUR COLUMN
         WHEN UPPER(TRIM(cst_gndr))= 'M' THEN 'Male'         --HERE WE APPLY TRIM()JSUT IN CASE SPACES APPEAR LATER IN YOUR COLUMN
         ELSE 'N/A'
    END cst_gndr, -- Normalize gender values to readable format
    cst_create_date
    FROM
    (
    SELECT * ,
        ROW_NUMBER()OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
    FROM Bronze.crm_cust_info)T WHERE flag_last =1;  -- Select the most recent record per customer
    SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';

    ---------------------------------------------------------------------------------------------------
    
		-- Loading silver.crm_prd_info
    SET @start_time = GETDATE();
    PRINT'>>Truncating Table Silver.crm_prd_info'
    TRUNCATE TABLE Silver.crm_prd_info;
    Print'>>Inserting Data Into:Silver.crm_prd_info'
    INSERT INTO Silver.crm_prd_info(
            prd_id,
            cat_id,
            prd_key,
            prd_nm,
            prd_cost,
            prd_line,
            prd_start_dt,
            prd_end_dt

     )
    select 
          prd_id,
 
          REPLACE(SUBSTRING(prd_key,1,5),'-','_') AS  cat_id,     --SUBSTRING() IT EXTRACTS A SPECIFIC PART OF A STRING
          SUBSTRING(prd_key,7,LEN(prd_key)) AS prd_key,           --LEN() IT RETURNS THE NUMBER OF CHARACTERS IN A STRING
          prd_nm,
          ISNULL(prd_cost,0) AS prd_cost ,                               --ISNULL()REPLACES NULL VALUES WITH A SPECIFIED REPLACEMENT VALUE OR WE CAN USE COALESCE ALSO
    
          CASE WHEN UPPER(TRIM(prd_line))='M' THEN 'Mountain'
               WHEN UPPER(TRIM(prd_line))='R' THEN 'Road'
               WHEN UPPER(TRIM(prd_line))='S'THEN 'Other Sales'
               WHEN UPPER(TRIM(prd_line))='T' THEN 'Touring'
               ELSE 'N/A'
         END AS prd_line,  --Map product line codes to descriptive vlaues
          CAST(prd_start_dt AS DATE) AS prd_start_dt,
          CAST(
          LEAD(prd_start_dt)OVER(PARTITION BY prd_Key ORDER BY prd_start_dt)-1 
          AS DATE
          ) AS  prd_end_dt --LEAD() ACCESS VLAUES FROM THE NEXT ROW WITHIN A WINDOW --CALCUALTE END DATE AS ONE DAY BEFORE THE NEXT START DATE
    FROM Bronze.crm_prd_info; 
    SET @end_time = GETDATE();
    PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
    PRINT '>> -------------';

    --SELECT distinct id from Bronze.erp_px_cat_g1v2;


    --SELECT
    --prd_id,                                               
    --prd_key,
    --prd_nm,
    --prd_start_dt,
    --prd_end_dt
    --FROM Bronze.crm_prd_info
    --WHERE prd_key IN('AC-HE-HL-U509-R', 'AC-HE-U509');

    --SELECT * FROM Silver.crm_prd_info;

    -----------------------------------------------------------------------------------------------------------------------------------------
    -- Loading crm_sales_details
    SET @start_time = GETDATE();
    PRINT'>>Truncating Table Silver.crm_sales_details'
    TRUNCATE TABLE Silver.crm_sales_details;
    Print'>>Inserting Data Into:Silver.crm_sales_details'
    INSERT INTO Silver.crm_sales_details(
           sls_ord_num,
           sls_prd_key,
           sls_cust_id,
           sls_order_dt,
           sls_ship_dt,
           sls_due_dt,
           sls_sales,
           sls_quantity,
           sls_price
    )
    SELECT
    sls_ord_num,
    sls_prd_key ,
    sls_cust_id,
     CASE WHEN sls_order_dt=0 OR LEN(sls_order_dt)!=8 THEN NULL
          ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
          END AS sls_order_dt,

    CASE WHEN sls_ship_dt=0 OR LEN(sls_ship_dt)!=8 THEN NULL
          ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
          END AS sls_ship_dt,

    CASE WHEN sls_due_dt=0 OR LEN(sls_due_dt)!=8 THEN NULL
          ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
          END AS sls_due_dt,


    CASE WHEN sls_sales IS NULL or sls_sales<=0 or sls_sales != sls_quantity * ABS(sls_price)  --abs()returns absolute value of numbmer measn negative to posit ive
         THEN sls_quantity * ABS(sls_price)
         ELSE sls_sales
    END AS sls_sales,  --RECALCULATE SALES IF ORIGINAL VALUE IS MISSING OR INCORRECT
    sls_quantity,

    CASE WHEN sls_price IS NULL OR sls_price<=0
         THEN sls_sales/ NULLIF(sls_quantity,0)
         ELSE sls_price  --DERIVE PRICE IF ORIGINAL VALUE IS INVALID
    END AS sls_price
    FROM Bronze.crm_sales_details;
    SET @end_time = GETDATE();
    PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
    PRINT '>> -------------';

    -----------------------------------------------------------------------------------------------------------------------------------------------------
        -- Loading erp_cust_az12
    SET @start_time = GETDATE();
    PRINT'>>Truncating Table Silver.erp_cust_az12'
    TRUNCATE TABLE Silver.erp_cust_az12;
    Print'>>Inserting Data Into:Silver.erp_cust_az12'
    INSERT INTO Silver.erp_cust_az12(cid,bdate,gen)
    SELECT 

    CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid,4,LEN(cid))   --'Remove 'NAS' prefix if present
         ELSE cid
    END AS cid,

    CASE WHEN bdate > GETDATE() then NULL  
         ELSE bdate
    END AS bdate, --Set future birthdates to NULL

    CASE WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
         WHEN UPPER(TRIM(gen)) IN ('M','MALE') THEN 'Male'
         ELSE 'N/A'
    END AS gen  -- Normalize gender values and handle unknown cases 
    FROM
    Bronze.erp_cust_az12;
    SET @end_time = GETDATE();
    PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
    PRINT '>> -------------';

	PRINT '------------------------------------------------';
	PRINT 'Loading ERP Tables';
	PRINT '------------------------------------------------';

    -------------------------------------------------------------------------------------------------------------
         -- Loading erp_loc_a101
    SET @start_time = GETDATE();
    PRINT'>>Truncating Table Silver.erp_loc_a101'
    TRUNCATE TABLE Silver.erp_loc_a101;
    Print'>>Inserting Data Into:Silver.erp_loc_a101'
    INSERT INTO Silver.erp_loc_a101 (cid,cntry)

    SELECT 
    REPLACE(cid,'-','')cid,
    CASE WHEN TRIM(cntry)= 'DE' THEN 'Germany'
         WHEN TRIM(cntry) IN ('US','USA') THEN 'United States'
         WHEN Trim(cntry)= '' OR cntry IS NULL THEN 'N/A'
         ELSE TRIM(cntry)
    END AS cntry  --Normalize and handle missing or blank country codes
    FROM Bronze.erp_loc_a101;
    SET @end_time = GETDATE();
    PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
    PRINT '>> -------------';
    --------------------------------------------------------------------------------------------------------------------
    	-- Loading erp_px_cat_g1v2
	SET @start_time = GETDATE();
    PRINT'>>Truncating Table Silver.erp_px_cat_g1v2'
    TRUNCATE TABLE Silver.erp_px_cat_g1v2
    Print'>>Inserting Data Into:Silver.erp_px_cat_g1v2'
    INSERT INTO Silver.erp_px_cat_g1v2 (id,cat,subcat,maintenance)
    SELECT 
    id,
    cat,
    subcat,
    maintenance
    FROM Bronze.erp_px_cat_g1v2;
    SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';

		SET @batch_end_time = GETDATE();
		PRINT '=========================================='
		PRINT 'Loading Silver Layer is Completed';
        PRINT '   - Total Load Duration: ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
		PRINT '=========================================='
		
	END TRY
	BEGIN CATCH
		PRINT '=========================================='
		PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER'
		PRINT 'Error Message' + ERROR_MESSAGE();
		PRINT 'Error Message' + CAST (ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error Message' + CAST (ERROR_STATE() AS NVARCHAR);
		PRINT '=========================================='
	END CATCH
END
