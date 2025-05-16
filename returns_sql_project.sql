
create database olist;
use olist;
-- Create orders table
CREATE TABLE orders (
    order_id VARCHAR(255) PRIMARY KEY,
    customer_id VARCHAR(255) NULL,
    order_status VARCHAR(255) NULL,
    order_purchase_timestamp TIMESTAMP NULL,
    order_approved_at TIMESTAMP NULL,
    order_delivered_carrier_date TIMESTAMP NULL,
    order_delivered_customer_date TIMESTAMP NULL,
    order_estimated_delivery_date TIMESTAMP NULL
);

CREATE TABLE order_items (
  order_id VARCHAR(255) NOT NULL,
  order_item_id INT NOT NULL,
  product_id VARCHAR(255) NULL,
  seller_id VARCHAR(255) NULL,
  shipping_limit_date TIMESTAMP NULL,
  price FLOAT NULL,
  freight_value FLOAT NULL,
  PRIMARY KEY (order_id, order_item_id)
);

  
CREATE TABLE products (
    product_id VARCHAR(255) ,
    product_category_name VARCHAR(255) NULL,
    product_name_length INT NULL,
    product_description_length INT NULL,
    product_photos_qty INT NULL,
    product_weight_g INT NULL,
    product_length_cm INT NULL,
    product_height_cm INT NULL,
    product_width_cm INT NULL
);


CREATE TABLE sellers (
    seller_id VARCHAR(255) PRIMARY KEY,
    seller_zip_code_prefix VARCHAR(255) NULL,
    seller_city VARCHAR(255) NULL,
    seller_state VARCHAR(255) NULL
);

--  CREATE USER 'root'@'DESKTOP-FHJBTK2' IDENTIFIED BY 'root';

-- GRANT ALL PRIVILEGES ON *.* TO 'root'@'DESKTOP-FHJBTK2' WITH GRANT OPTION;

-- FLUSH PRIVILEGES; 

-- Standardize and remove nulls --

set sql_safe_updates=0;


-- orders -- 
-- Remove rows with critical NULLs
DELETE FROM orders
WHERE order_purchase_timestamp IS NULL
   OR order_estimated_delivery_date IS NULL
   OR order_delivered_customer_date IS NULL;

-- Remove extreme outliers: delivery took more than 60 days
DELETE FROM orders
WHERE DATEDIFF(order_delivered_customer_date, order_purchase_timestamp) > 60;

-- removing duplicates--

WITH ranked_orders AS (
  SELECT order_id,
         ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY order_purchase_timestamp) AS rn
  FROM orders
)
DELETE FROM orders
WHERE order_id IN (
  SELECT order_id FROM ranked_orders WHERE rn > 1
);

-- order_items--
-- Remove rows with NULL or zero/negative price or freight
DELETE FROM order_items
WHERE price IS NULL OR price <= 0
   OR freight_value IS NULL OR freight_value < 0;

--  Remove rows with NULL shipping limit date 
DELETE FROM order_items
WHERE shipping_limit_date IS NULL;

--  Check duplicates (order_id + item_id should be unique)
SELECT order_id, order_item_id, COUNT(*)
FROM order_items
GROUP BY order_id, order_item_id
HAVING COUNT(*) > 1;

 -- products--
 -- Remove rows with invalid weight or dimensions
DELETE FROM products
WHERE product_weight_g IS NULL OR product_weight_g <= 0
   OR product_length_cm IS NULL OR product_length_cm <= 0
   OR product_height_cm IS NULL OR product_height_cm <= 0
   OR product_width_cm IS NULL OR product_width_cm <= 0;

-- Fill unknown category names with a placeholder
UPDATE products
SET product_category_name = 'unknown'
WHERE product_category_name IS NULL;

--  Fill missing photo or description lengths with 0
UPDATE products
SET product_photos_qty = 0
WHERE product_photos_qty IS NULL;

UPDATE products
SET product_name_length = 0
WHERE product_name_length IS NULL;

UPDATE products
SET product_description_length = 0
WHERE product_description_length IS NULL;

-- sellers-- 
-- Remove rows with NULL seller_id (shouldn't happen but safe check)
DELETE FROM sellers
WHERE seller_id IS NULL;

-- Normalize city and state strings
UPDATE sellers
SET 
  seller_city = LOWER(TRIM(seller_city)),
  seller_state = UPPER(TRIM(seller_state));

-- Fill unknowns if city/state missing
UPDATE sellers
SET seller_city = 'unknown'
WHERE seller_city IS NULL OR seller_city = '';

UPDATE sellers
SET seller_state = 'NA'
WHERE seller_state IS NULL OR seller_state = '';

-- queries--
-- Query 1: Supplier Delay Rate
SELECT 
    s.seller_id,
    s.seller_city,
    COUNT(*) AS total_orders,
    COUNT(CASE WHEN o.order_delivered_customer_date > o.order_estimated_delivery_date THEN 1 END) AS delayed_orders,
    ROUND(100.0 * COUNT(CASE WHEN o.order_delivered_customer_date > o.order_estimated_delivery_date THEN 1 END) / COUNT(*), 2) AS delay_rate_pct
FROM orders o
JOIN order_items oi ON o.order_id = oi.order_id
JOIN sellers s ON oi.seller_id = s.seller_id
WHERE o.order_status = 'delivered'
GROUP BY s.seller_id, s.seller_city
ORDER BY delay_rate_pct DESC;

-- Query 2: Avg Delivery Delay by Region
SELECT 
    s.seller_state,
    ROUND(AVG(DATEDIFF(o.order_delivered_customer_date, o.order_estimated_delivery_date)), 2) AS avg_delay_days
FROM orders o
JOIN order_items oi ON o.order_id = oi.order_id
JOIN sellers s ON oi.seller_id = s.seller_id
WHERE o.order_status = 'delivered' 
  AND o.order_delivered_customer_date > o.order_estimated_delivery_date
GROUP BY s.seller_state
ORDER BY avg_delay_days DESC;

-- Query 3: Total Delivery Time (Customer Wait Time)
SELECT 
    ROUND(AVG(DATEDIFF(o.order_delivered_customer_date, o.order_purchase_timestamp)), 2) AS avg_customer_wait_days
FROM orders o
WHERE o.order_status = 'delivered';

--  Query 4: Risk Score per Seller
SELECT 
    s.seller_id,
    COUNT(*) AS total_orders,
    ROUND(SUM(CASE WHEN o.order_delivered_customer_date > o.order_estimated_delivery_date THEN 1 ELSE 0 END) * 1.0, 2) AS delayed_count,
    ROUND((SUM(CASE WHEN o.order_delivered_customer_date > o.order_estimated_delivery_date THEN 1 ELSE 0 END) * 1.0 / COUNT(*)) * 100, 2) AS delay_rate_pct,
    ROUND(SUM(CASE WHEN o.order_delivered_customer_date > o.order_estimated_delivery_date THEN 1 ELSE 0 END) * COUNT(*), 2) AS supplier_risk_score
FROM orders o
JOIN order_items oi ON o.order_id = oi.order_id
JOIN sellers s ON oi.seller_id = s.seller_id
WHERE o.order_status = 'delivered'
GROUP BY s.seller_id
ORDER BY supplier_risk_score DESC;

-- Query 5: Most Delay-Prone Product Categories
SELECT 
    p.product_category_name,
    COUNT(*) AS total_orders,
    COUNT(CASE WHEN o.order_delivered_customer_date > o.order_estimated_delivery_date THEN 1 END) AS delays,
    ROUND(100.0 * COUNT(CASE WHEN o.order_delivered_customer_date > o.order_estimated_delivery_date THEN 1 END) / COUNT(*), 2) AS delay_rate
FROM orders o
JOIN order_items oi ON o.order_id = oi.order_id
JOIN products p ON oi.product_id = p.product_id
WHERE o.order_status = 'delivered'
GROUP BY p.product_category_name
HAVING COUNT(*) > 20
ORDER BY delay_rate DESC;

-- Query 6: High Freight Cost Sellers
SELECT 
    s.seller_id,
    ROUND(AVG(oi.freight_value), 2) AS avg_freight,
    COUNT(*) AS total_orders
FROM order_items oi
JOIN sellers s ON oi.seller_id = s.seller_id
GROUP BY s.seller_id
HAVING AVG(oi.freight_value) > 30
ORDER BY avg_freight DESC;

 -- Query 7: Delay Trend by Month
 SELECT 
    DATE_FORMAT(o.order_purchase_timestamp, '%Y-%m') AS order_month,
    COUNT(*) AS total_orders,
    COUNT(CASE WHEN o.order_delivered_customer_date > o.order_estimated_delivery_date THEN 1 END) AS delayed_orders
FROM orders o
WHERE o.order_status = 'delivered'
GROUP BY order_month
ORDER BY order_month;

 -- Query 8: Avg Delivery Time by Product Category
 SELECT 
    p.product_category_name,
    ROUND(AVG(DATEDIFF(o.order_delivered_customer_date, o.order_purchase_timestamp)), 2) AS avg_fulfillment_days
FROM orders o
JOIN order_items oi ON o.order_id = oi.order_id
JOIN products p ON oi.product_id = p.product_id
WHERE o.order_status = 'delivered'
GROUP BY p.product_category_name
ORDER BY avg_fulfillment_days DESC;

 














