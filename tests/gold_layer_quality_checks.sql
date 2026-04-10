/*
===============================================================================
Gold Layer – Quality Validation Checks
===============================================================================
Purpose:
    This script validates the correctness and reliability of the Gold Layer.
    These checks ensure:
        • Surrogate keys in dimensions are unique.
        • No duplicated business keys after transformations.
        • Fact table maintains valid relationships with all dimensions.
        • No broken links or missing references in the star schema.

Notes:
    - Any returned rows indicate a data quality issue that must be investigated.
    - Expected behavior: All checks should return zero rows.
===============================================================================
*/


-- ====================================================================
-- 1) Validate Dimension: gold.dim_customers
-- ====================================================================

-- Check uniqueness of surrogate key
SELECT 
    customer_key,
    COUNT(*) AS occurrences
FROM gold.dim_customers
GROUP BY customer_key
HAVING COUNT(*) > 1;


-- Check uniqueness of business key (customer_id)
SELECT 
    customer_id,
    COUNT(*) AS occurrences
FROM gold.dim_customers
GROUP BY customer_id
HAVING COUNT(*) > 1;



-- ====================================================================
-- 2) Validate Dimension: gold.dim_products
-- ====================================================================

-- Check uniqueness of surrogate key
SELECT 
    product_key,
    COUNT(*) AS occurrences
FROM gold.dim_products
GROUP BY product_key
HAVING COUNT(*) > 1;

-- Check uniqueness of business key (product_number)
SELECT 
    product_number,
    COUNT(*) AS occurrences
FROM gold.dim_products
GROUP BY product_number
HAVING COUNT(*) > 1;



-- ====================================================================
-- 3) Validate Fact Table: gold.fact_sales
-- ====================================================================

-- Check for missing dimension references (broken foreign keys)
SELECT 
    f.order_number,
    f.product_key,
    f.customer_key
FROM gold.fact_sales f
LEFT JOIN gold.dim_products p
    ON f.product_key = p.product_key
LEFT JOIN gold.dim_customers c
    ON f.customer_key = c.customer_key
WHERE p.product_key IS NULL 
   OR c.customer_key IS NULL;


-- Optional: Check for duplicated fact rows (should not happen)
SELECT 
    order_number,
    product_key,
    customer_key,
    COUNT(*) AS occurrences
FROM gold.fact_sales
GROUP BY order_number, product_key, customer_key
HAVING COUNT(*) > 1;
