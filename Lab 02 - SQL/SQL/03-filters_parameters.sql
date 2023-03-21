-- FOR FILTERING
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
  b.name;


-- FOR PARAMETERS
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
HAVING cnt > 5 -- replace this with a parameter
