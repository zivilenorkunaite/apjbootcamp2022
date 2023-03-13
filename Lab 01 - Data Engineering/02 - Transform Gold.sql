-- Databricks notebook source
CREATE LIVE TABLE country_sales
select l.country_code, sum(product_cost) as total_sales, count(distinct sale_id) as number_of_sales
from live.silver_sales_items s 
  join live.silver_stores l on s.store_id = l.id
group by l.country_code;
