-- BRONZE TABLE -1 
--1 QUALITY CHECK
--Check For Nulls or Duplicates in Primary Key
-- Expectation: No result

SELECT 
cst_id,
COUNT(*)
FROM Bronze.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*)>1 OR cst_id IS NULL;

--2 QUALITY CHECK 
 --CHECK FOR UNWANTED SPACES in string values 
 -- TRIM() RMEOVES LEADING AND TRAILING SPACES FROM STRING
 --EXPECTATION:NO REUSLTS 
 Select  cst_lastname
 FROM Bronze.crm_cust_info
 WHERE cst_lastname != TRIM(cst_lastname);    -- IF THE ORIGINAL VALUE IS NOT EQUAL TO
                                             -- THE SAME VALUE AFTER TRIMMING, IT MEANS  THERE ARE SPACES!





--3.1 QUALITY CHECK
--CHECK THE CONSISTENCY OF VALUES IN LOW CARDINALITY COLUMNS 
--THIS FOR GENDER YOU KNOW MALE OR FEMALE SO LOW CARDINALITY COLUMNS

SELECT DISTINCT cst_gndr                     --IN OUR DATA WAREHOUSE, WE AIM TO STORE CLEAR & MEANINGFUL VALUES
from Bronze.crm_cust_info                    --RATHER THAN USING ABBREVIATED TERMS FOR EXAMPLE 'MALE' INSTEAD OF 'M'
                                                                                            -- 'FEMALE' INSTEAD OF 'F'
                                             -- IN OUR DATA WAREHOUSE, WE USE THE DEFAULT VALUE 'N/A' FOR MISSING VALUES!
--3.2 QUALITY CHECK
--CHECK THE CONSISTENCY OF VALUES IN LOW CARDINALITY COLUMNS 
--THIS FOR MATERIAL_STATUS YOU KNOW SINGLE OR MARRIED SO LOW CARDINALITY COLUMNS

SELECT DISTINCT cst_material_status                     --IN OUR DATA WAREHOUSE, WE AIM TO STORE CLEAR & MEANINGFUL VALUES
from Bronze.crm_cust_info                    --RATHER THAN USING ABBREVIATED TERMS FOR EXAMPLE 'Married' INSTEAD OF 'M'
                                                                                            -- 'SINGLE' INSTEAD OF 'S'
                                             -- IN OUR DATA WAREHOUSE, WE USE THE DEFAULT VALUE 'N/A' FOR MISSING VALUES!


--BRONZE TABLE-2
--THIS IS FOR OTHER TABLE BRONZE.CRM_PRD_INFO
--1 QUALITY CHECK
--Check For Nulls or Duplicates in Primary Key
-- Expectation: No result
SELECT 
prd_id,
COUNT(*)
FROM Bronze.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*)>1 OR prd_id IS NULL;

SELECT * FROM Bronze.crm_prd_info;

--CHECK FOR UNWANTED SPACES
--EXPECATATION:RESULT
SELECT prd_nm
FROM Bronze.crm_prd_info
WHERE prd_nm!=TRIM(prd_nm);

--CHECK FOR NULLS OR NEGATIVE NUMBERS
--EXPECATION: NO RESULTS
SELECT prd_cost
FROM Bronze.crm_prd_info
WHERE prd_cost <0 OR prd_cost IS NULL;

--DATA STANDARDIZATION & CONSISTENCY
SelecT DISTINCT prd_line
FROM Bronze.crm_prd_info;

--CHECK FOR INVALID DATE ORDERS
SELECT *                        --END DATE MUST NOT BE EARLIER THAN THE START DATE 
FROM Bronze.crm_prd_info         --FOR COMPLEX TRANSFORMATIONS IN SQL,I MEAN(BARRA) NARROW IT DOWN TO A SPECIFIC EXAMPLE &BRAINSTORM,MULTIPLE SOLUTION APRROACHES
WHERE prd_end_dt < prd_start_dt



--BRONZE TABLE-3 

--CHECK FOR INVALID DATES

SELECT 
    NULLIF(sls_order_dt,0)        -- NULLIF()RETURNS NULL IF 2 GIVEN VALUES ARE EQUAL;OTHERWISE,IT RETURNS THE FIRST EXPRESSION                            
    FROM Bronze.crm_sales_details --NEGATIVE NUMBERS OR ZEROS CANT BE CAST TO A DATE CHECK FOR BOTH NEGATIVES AND 0 IF THERE ARE SOME SOLVE
    WHERE sls_order_dt <=0 
    OR LEN(sls_order_dt)!=8         --HERE LENGTH OF DATE IS 8 EXAMPLE YEAR|MONTH|DATE IF LESS THAN 8 OR GREATER THAN 8 PROBELM BAD QUALITY DATA SOLVE
    OR sls_order_dt >20500101 
    OR sls_order_dt< 19000101       --CHECK FOR OUTLIERS BY VALIDATING THE BOUNDARIES OF DATAE RANGE EXAMPLE sls_order_dt>20500101


SELECT 
    NULLIF (sls_ship_dt,0)        -- NULLIF()RETURNS NULL IF 2 GIVEN VALUES ARE EQUAL;OTHERWISE,IT RETURNS THE FIRST EXPRESSION                            
    FROM Bronze.crm_sales_details --NEGATIVE NUMBERS OR ZEROS CANT BE CAST TO A DATE CHECK FOR BOTH NEGATIVES AND 0 IF THERE ARE SOME SOLVE
    WHERE  sls_ship_dt <=0 
    OR LEN( sls_ship_dt)!=8         --HERE LENGTH OF DATE IS 8 EXAMPLE YEAR|MONTH|DATE IF LESS THAN 8 OR GREATER THAN 8 PROBELM BAD QUALITY DATA SOLVE
    OR  sls_ship_dt >20500101 
    OR  sls_ship_dt< 19000101       --CHECK FOR OUTLIERS BY VALIDATING THE BOUNDARIES OF DATAE RANGE EXAMPLE sls_order_dt>20500101

SELECT 
    NULLIF (sls_due_dt,0)        -- NULLIF()RETURNS NULL IF 2 GIVEN VALUES ARE EQUAL;OTHERWISE,IT RETURNS THE FIRST EXPRESSION                            
    FROM Bronze.crm_sales_details --NEGATIVE NUMBERS OR ZEROS CANT BE CAST TO A DATE CHECK FOR BOTH NEGATIVES AND 0 IF THERE ARE SOME SOLVE
    WHERE  sls_due_dt <=0 
    OR LEN( sls_due_dt)!=8         --HERE LENGTH OF DATE IS 8 EXAMPLE YEAR|MONTH|DATE IF LESS THAN 8 OR GREATER THAN 8 PROBELM BAD QUALITY DATA SOLVE
    OR  sls_due_dt >20500101 
    OR  sls_due_dt< 19000101       --CHECK FOR OUTLIERS BY VALIDATING THE BOUNDARIES OF DATAE RANGE EXAMPLE sls_order_dt>20500101


--check for invalid date orders
SELECT 
*FROM Bronze.crm_sales_details
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
sls_price,
CASE WHEN sls_sales IS NULL or sls_sales<=0 or sls_sales != sls_quantity * ABS(sls_price)  --abs()returns absolute value of numbmer measn negative to posit ive
     THEN sls_quantity * ABS(sls_price)
     ELSE sls_sales
END AS sls_sales,
CASE WHEN sls_price IS NULL OR sls_price<=0
     THEN sls_sales/ NULLIF(sls_quantity,0)
     ELSE sls_price 
END AS sls_price
FROM Bronze.crm_sales_details



--BRONZE TABLE -4
-- IDENTIFY OUT-OF RANGE DATES

SELECT DISTINCT
bdate
FROM Bronze.erp_cust_az12
WHERE bdate<'1924-01-01' OR bdate>GETDATE()                              --here check for very old customers
                                                                            --hERE CHeck for birthdays in the future

--Data Standardization & Consistency
SELECT DISTINCT gen
From Bronze.erp_cust_az12



--BRONZE TABLE-5
--DATA Standardization & consistency
SELECT DISTINCT cntry
FROM Bronze.erp_loc_a101
ORDER BY cntry;

--BRONZE TABLE-6
--check For unwanted Spaces
SELECT *  FROM Bronze.erp_px_cat_g1v2
WHERE cat!= TRIM(cat) OR subcat !=TRIM(subcat) OR maintenance != TRIM (maintenance)

--data standardization & consistency 
SELECT DISTINCT*FROM Bronze.erp_px_cat_g1v2
 
