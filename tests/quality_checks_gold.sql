/*
===============================================================================
Quality Checks
===============================================================================
Script Purpose:
    This script performs quality checks to validate the integrity, consistency, 
    and accuracy of the Gold Layer. These checks ensure:
    - Uniqueness of surrogate keys in dimension tables.
    - Referential integrity between fact and dimension tables.
    - Validation of relationships in the data model for analytical purposes.

Usage Notes:
    - Investigate and resolve any discrepancies found during the checks.
===============================================================================
*/

-- ====================================================================
-- Checking 'gold.dim_customers'
-- ====================================================================
-- Check for Uniqueness of Customer Key in gold.dim_customers
-- Expectation: No results 

--HERE WE ARE USING SUBQUERY TO CHECK 
--TIP
--After Joining table, check if any duplicates were introduced by the join logic because one to many relationship or many to many may happen

SELECT cst_id, COUNT(*) FROM

(SELECT 
ci.cst_id,
ci.cst_key,                                                                                          
ci.cst_firstname,
ci.cst_lastname,
ci.cst_marital_status,
ci.cst_gndr,
ci.cst_create_date,
ca.bdate,
ca.gen,
la.cntry
FROM  Silver.crm_cust_info ci
LEFT JOIN Silver.erp_cust_az12  ca
ON ci.cst_key= ca.cid
LEFT JOIN Silver.erp_loc_a101 la 
ON ci.cst_key= la.cid)t 
GROUP BY cst_id 
HAVING COUNT(*)>1;

--other wAY
SELECT 
    customer_key,
    COUNT(*) AS duplicate_count
FROM gold.dim_customers
GROUP BY customer_key
HAVING COUNT(*) > 1;



-- ====================================================================
-- Checking 'gold.product_key'
-- ====================================================================
-- Check for Uniqueness of Product Key in gold.dim_products
-- Expectation: No results 

--CHECKING QULAITY OF GOLLD CHIECKING UNIQUENESS
SELECT prd_key,COUNT(*)
FROM (
SELECT
pn.prd_id,
pn.cat_id,
pn.prd_key,
pn.prd_cost,
pn.prd_line,
pn.prd_start_dt,
pc.cat,
pc.maintenance
FROM Silver.crm_prd_info pn
LEFT JOIN Silver.erp_px_cat_g1v2 pc
ON pn.cat_id= pc.id
WHERE prd_end_dt IS NULL  -- Filter out all historical data
)t GROUP BY prd_key 
HAVING COUNT(*)>1;

--other way
SELECT 
    product_key,
    COUNT(*) AS duplicate_count
FROM gold.dim_products
GROUP BY product_key
HAVING COUNT(*) > 1;

-- ====================================================================
-- Checking 'gold.fact_sales'
-- ====================================================================
-- Check the data model connectivity between fact and dimensions

--foreign key integrity (dimensions)
-- fact check check if all dimension tables can successfully join to the fact table 
SELECT * FROM Gold.fact_sales f
LEFT JOIN  Gold.dim_customers c
ON c.customer_key= f.customer_key
WHERE c.customer_key IS  NULL;

--check for not matching rows which shows flaws in data 
SELECT * FROM Gold.fact_sales f
LEFT JOIN  Gold.dim_products p
ON p.product_key= f.product_key
WHERE p.product_key IS  NULL;

--other way
SELECT * 
FROM gold.fact_sales f
LEFT JOIN gold.dim_customers c
ON c.customer_key = f.customer_key
LEFT JOIN gold.dim_products p
ON p.product_key = f.product_key
WHERE p.product_key IS NULL OR c.customer_key IS NULL  

