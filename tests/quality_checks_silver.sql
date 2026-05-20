/*
===============================================================================
Quality Checks
===============================================================================
Script Purpose:
    This script performs various quality checks for data consistency, accuracy, 
    and standardization across the 'silver' layer. It includes checks for:
    - Null or duplicate primary keys.
    - Unwanted spaces in string fields.
    - Data standardization and consistency.
    - Invalid date ranges and orders.
    - Data consistency between related fields.

Usage Notes:
    - Run these checks after data loading Silver Layer.
    - Investigate and resolve any discrepancies found during the checks.
===============================================================================
*/

-- ====================================================================
-- Checking 'silver.crm_cust_info'
-- ====================================================================
-- Check for NULLs or Duplicates in Primary Key
-- Expectation: No Results
-- SIVLER TABLE
-- here we re-run the quality check queries from the bronze layer to verify the quality of data in the silver layer.

--------REMOVE UNWANTED SPACES-------------------
---REMOVES UNNECESSAR SPACES TO ENSURE DATA CONSISTENCY,AND UNIFORMITY ACROSS ALL RECORDS.  

--------DATA NORMALIZATION &STANDARDIZATION---------
--MAPS CODED VALUES TO MEANINGFUL,USER-FRIENDLY DESCRIPTIONS.

------ HANDLING MISSING DATA-----------------------
--FILLS IN THE BLANKS BY ADDING A DEFAULT VALUE

---- REMOVE DUPLICATES---------------------------
---ENSURE ONLY ONE RECORD PER ENTITY BY IDENTIFYING AND RETAINING THE MOST RELEVANT ROW.


--SILVER TABLE -1
--1 QUALITY CHECK
--Check For Nulls or Duplicates in Primary Key
-- Expectation: No result

SELECT 
cst_id,
COUNT(*)
FROM Silver.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*)>1 OR cst_id IS NULL;

--2 QUALITY CHECK 
 --CHECK FOR UNWANTED SPACES in string values 
 -- TRIM() RMEOVES LEADING AND TRAILING SPACES FROM STRING
 --EXPECTATION:NO REUSLTS 
 Select  cst_lastname
 FROM Silver.crm_cust_info
 WHERE cst_lastname != TRIM(cst_lastname);    -- IF THE ORIGINAL VALUE IS NOT EQUAL TO
                                             -- THE SAME VALUE AFTER TRIMMING, IT MEANS  THERE ARE SPACES!


                                              


--3.1 QUALITY CHECK
--CHECK THE CONSISTENCY OF VALUES IN LOW CARDINALITY COLUMNS 
--THIS FOR GENDER YOU KNOW MALE OR FEMALE SO LOW CARDINALITY COLUMNS

SELECT DISTINCT cst_gndr                     --IN OUR DATA WAREHOUSE, WE AIM TO STORE CLEAR & MEANINGFUL VALUES
from Silver.crm_cust_info;                   --RATHER THAN USING ABBREVIATED TERMS FOR EXAMPLE 'MALE' INSTEAD OF 'M'
                                                                                          -- 'FEMALE' INSTEAD OF 'F'
                                             -- IN OUR DATA WAREHOUSE, WE USE THE DEFAULT VALUE 'N/A' FOR MISSING VALUES!
--3.2 QUALITY CHECK
--CHECK THE CONSISTENCY OF VALUES IN LOW CARDINALITY COLUMNS 
--THIS FOR MATERIAL_STATUS YOU KNOW SINGLE OR MARRIED SO LOW CARDINALITY COLUMNS

SELECT DISTINCT cst_marital_status                     --IN OUR DATA WAREHOUSE, WE AIM TO STORE CLEAR & MEANINGFUL VALUES
from Silver.crm_cust_info                    --RATHER THAN USING ABBREVIATED TERMS FOR EXAMPLE 'Married' INSTEAD OF 'M'
                                                                                            -- 'SINGLE' INSTEAD OF 'S'
                                             -- IN OUR DATA WAREHOUSE, WE USE THE DEFAULT VALUE 'N/A' FOR MISSING VALUES!




--SILVER TABLE-2
--THIS IS FOR OTHER TABLE silver.CRM_PRD_INFO
--DERIVED COLUMNS:-  Create a new columns based on calculations or transformations of existing ones
--DATA ENRICHMENT:- Add new,relevant data to enhance the dataset for analysis 

--1 QUALITY CHECK
--Check For Nulls or Duplicates in Primary Key
-- Expectation: No result

SELECT *FROM Silver.crm_cust_info

SELECT 
prd_id,
COUNT(*)
FROM Silver.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*)>1 OR prd_id IS NULL;
 
SELECT * FROM Bronze.crm_prd_info;

--CHECK FOR UNWANTED SPACES
--EXPECATATION:RESULT
SELECT prd_nm
FROM Silver.crm_prd_info
WHERE prd_nm!=TRIM(prd_nm);

--CHECK FOR NULLS OR NEGATIVE NUMBERS
--EXPECATION: NO RESULTS
SELECT prd_cost
FROM Silver.crm_prd_info
WHERE prd_cost <0 OR prd_cost IS NULL;

--DATA STANDARDIZATION & CONSISTENCY
SelecT DISTINCT prd_line
FROM Silver.crm_prd_info;

--CHECK FOR INVALID DATE ORDERS
SELECT *                        --END DATE MUST NOT BE EARLIER THAN THE START DATE 
FROM Silver.crm_prd_info         --FOR COMPLEX TRANSFORMATIONS IN SQL,I MEAN(BARRA) NARROW IT DOWN TO A SPECIFIC EXAMPLE &BRAINSTORM,MULTIPLE SOLUTION APRROACHES
WHERE prd_end_dt < prd_start_dt



 
--SILVER TABLE -3

--check for invalid date orders
SELECT 
*FROM Silver.crm_sales_details
WHERE sls_order_dt > sls_ship_dt OR sls_order_dt > sls_due_dt;


------BUSINESS RULES ANY WHERE
--- SALES = QUANTIY * PRICE
-- NEGATIVES, ZEROS ,NULLS ARE NOT ALLOWED
-- #1 SOLUTION
-- Data issues will be fixed direct in source system
--#2 solution
-- Data issues has to be fixed in data warehouse
-- RULESSS------------
--1. If sales is negative,zero,or null, derive it using quantity and price.
--2. If price is zero or null, calculate it using Sales and quantity.
--3. If price is negative,convert it to a positive value.
SELECT DISTINCT
sls_sales,
sls_quantity,
sls_price
FROM Silver.crm_sales_details
WHERE sls_sales != sls_quantity *sls_price
OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
Or sls_sales <=0 OR sls_quantity <=0 OR sls_price <=0
ORDER BY sls_sales,sls_quantity, sls_price;

SELECT * FROM Silver.crm_sales_details


--SILVER TABLE-4
-- IDENTIFY OUT-OF RANGE DATES

SELECT DISTINCT
bdate
FROM Silver.erp_cust_az12
WHERE bdate<'1924-01-01' OR bdate>GETDATE()                              --here check for very old customers
                                                                            --hERE CHeck for birthdays in the future

--Data Standardization & Consistency
SELECT DISTINCT gen
From Silver.erp_cust_az12




--SILVER-5
--DATA Standardization & consistency
SELECT DISTINCT cntry
FROM Silver.erp_loc_a101
ORDER BY cntry;
SELECT * FROM Silver.erp_loc_a101

--SILVER TABLE-6
--check For unwanted Spaces
SELECT *  FROM Silver.erp_px_cat_g1v2
WHERE cat!= TRIM(cat) OR subcat !=TRIM(subcat) OR maintenance != TRIM (maintenance)

--data standardization & consistency 
SELECT DISTINCT*FROM Silver.erp_px_cat_g1v2;
 
