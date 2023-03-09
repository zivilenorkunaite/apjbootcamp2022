-- Databricks notebook source
select count(1) sales_last_7_days from apjdatabricksbootcamp.silver_sales
where ts >= date_add(now(), -7)
and store_id = 'AKL01'
