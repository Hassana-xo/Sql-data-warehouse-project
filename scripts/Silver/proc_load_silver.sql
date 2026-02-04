/*
==============================================================================
Load Silver Layer (Bronze -> Silver)
==============================================================================
Script Purpose
		  This stored procedure performs the ETL (Extract, Transform, Load) process 
      to populate the 'Silver' Schema tables from 'bronze' schema.
Actions Performed:
    - Truncate Silver Tables
    - Inserts transformed and clensed data from Bronze into Silver tables.

Parameters:
    None.
    This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC Silver.load_silver;
==============================================================================
/*


TRUNCATE TABLE silver.crm_cust_info;
INSERT INTO silver.crm_cust_info(
cst_id, cst_key, cst_firstname,
cst_lastname, cst_marital_status,
cst_gndr, cst_create_date)
SELECT 
 cst_id,
 cst_key,
    TRIM(cst_firstname) AS cst_firstname,
    TRIM(cst_lastname) AS cst_lastname,
	CASE WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
    WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
    else 'n/a'
    end cst_marital_status,
    CASE WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
    WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
    else 'n/a'
    end cst_gndr,
    CASE 
    WHEN cst_create_date > 0000-00-00 THEN cst_create_date
    ELSE NULL
END cst_create_date
FROM
(SELECT *,
ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
FROM Bronze.crm_cust_info) T
WHERE flag_last = 1  AND cst_id is not null;


TRUNCATE TABLE silver.crm_prd_info;
INSERT INTO silver.crm_prd_info(
prd_id,
cat_id, prd_key,
prd_nm,
prd_cost,
prd_line, prd_start_dt,prd_end_dt)
SELECT prd_id,
replace(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,
SUBSTRING(prd_key, 7, length(prd_key)) AS prd_key,
prd_nm,
prd_cost,
CASE WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'
WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road'
WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'Other Sales'
WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
ELSE 'n/a'
END prd_line,
DATE(prd_start_dt) AS prd_start_dt,
    DATE(DATE_SUB(LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt), INTERVAL 1 DAY)) AS prd_end_dt
FROM Bronze.crm_prd_info;


TRUNCATE TABLE silver.crm_sales_details;
INSERT INTO silver.crm_sales_details(
sls_ord_num,
sls_prd_key,
sls_cust_id, sls_order_dt, 
sls_ship_dt,sls_due_dt, 
sls_sales, sls_quantity,sls_price)
SELECT 
sls_ord_num,
sls_prd_key,
sls_cust_id,
CASE WHEN sls_order_dt = 0 THEN NULL
	 ELSE sls_order_dt
	 END sls_order_dt,
CASE WHEN sls_ship_dt = 0 THEN NULL
	 ELSE sls_ship_dt
	 END sls_ship_dt,
CASE WHEN sls_due_dt = 0 THEN NULL
	 ELSE sls_due_dt
	 END sls_due_dt,
CASE WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantitY * ABS(sls_price) 
	 THEN sls_quantitY * ABS(sls_price) 
	 ELSE sls_sales
	 END sls_sales,
     sls_quantity,
CASE WHEN sls_price IS NULL OR sls_price <= 0 
	 THEN ROUND(sls_sales / NULLIF(sls_quantity, 0))
	 ELSE sls_price
	 END sls_price
FROM bronze.crm_sales_details;


TRUNCATE TABLE silver.erp_cust_az12;
INSERT INTO silver.erp_cust_az12(
CID, bdate,gen)
select
 CASE WHEN CID LIKE 'NAS%' THEN SUBSTRING(cid, 4, length(cid))
     ELSE CID
     END cid,
CASE WHEN bdate > CURDATE() THEN NULL
	 ELSE bdate
END AS bdate,
CASE WHEN TRIM(gen) LIKE 'F%' THEN 'Female'
	 WHEN TRIM(gen) LIKE 'M%' THEN 'Male'
    ELSE 'n/a'
    END GEN
FROM bronze.erp_cust_az12;


TRUNCATE TABLE silver.erp_loc_a101;
INSERT INTO silver.erp_loc_a101(
cid, cntry)
SELECT
REPLACE(cid, '-', '') as cid,
CASE WHEN TRIM(CNTRY) LIKE 'Australia%' THEN 'Australia'
	 WHEN TRIM(CNTRY) LIKE 'GERMANY%' THEN 'Germany'
	 WHEN TRIM(CNTRY) LIKE 'DE%' THEN 'Germany'
	 WHEN TRIM(CNTRY) LIKE 'Canada%' THEN 'Canada'
	 WHEN TRIM(CNTRY) LIKE 'United Kingdom%' THEN 'United Kingdom'
     WHEN TRIM(CNTRY) LIKE 'United States%' THEN 'United States'
     WHEN TRIM(CNTRY) LIKE 'Us%' THEN 'United States'
     WHEN TRIM(CNTRY) LIKE 'France%' THEN 'France'
     ELSE 'n/a'
     END cntry
    FROM BRONZE.erp_loc_a101;
    
    
 TRUNCATE TABLE silver.erp_px_cat_g1v2;
INSERT INTO silver.erp_px_cat_g1v2(
id, cat, subcat, maintenance)
SELECT id, cat, subcat, maintenance
FROM bronze.erp_px_cat_g1v2; 

SELECT *
FROM silver.erp_px_cat_g1v2;   
    
