--------------------
/* replace your_database_name in the following command by your database name 
get your databse name by running the first cell of the prep-notebook 
*/
--------------------
USE your_database_name;
--------------------

SELECT
  a.store_id,
  a.order_source,
  a.order_state,
  b.city,
  b.country_code,
  b.name AS store_name,
  count(*) AS cnt
FROM
  fact_apj_sales a
  JOIN dim_store_locations b ON a.slocation_skey = b.slocation_skey
GROUP BY
  a.store_id,
  a.order_source,
  a.order_state,
  b.city,
  b.country_code,
  b.name


-- CREATE a VIEW

CREATE VIEW IF NOT EXISTS vw_order_by_city
AS
SELECT
  a.store_id,
  a.order_source,
  a.order_state,
  b.city,
  b.country_code,
  b.name AS store_name,
  count(*) AS cnt
FROM
  fact_apj_sales a
  JOIN dim_store_locations b ON a.slocation_skey = b.slocation_skey
GROUP BY
  a.store_id,
  a.order_source,
  a.order_state,
  b.city,
  b.country_code,
  b.name
;
  
SELECT * FROM vw_order_by_city

