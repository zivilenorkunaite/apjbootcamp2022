--------------------
/* replace your_database_name in the following command by your database name 
get your databse name by running the first cell of the prep-notebook 
*/
--------------------
USE your_database_name;
--------------------

SHOW TABLES;

SELECT  * FROM fact_apj_sales;

DESCRIBE DETAIL fact_apj_sales;

DESCRIBE HISTORY fact_apj_sales;

DESCRIBE EXTENDED fact_apj_sales;


INSERT INTO fact_apj_sales
(customer_skey, slocation_skey, sale_id, ts, order_source, order_state,  unique_customer_id, store_id) 
VALUES
("3157","7" , "00009e08-3343-4a88-b40d-a66fede2cdff", current_timestamp() ,"IN-STORE","COMPLETED","SYD01-15", "SYD01"),
("3523","5","00041cc6-30f1-433d-97b5-b92191a92efb",current_timestamp(),"ONLINE","COMPLETED","SYD01-48", "SYD01");


SELECT  count(*) FROM fact_apj_sales;

SELECT * FROM fact_apj_sales ORDER BY ts DESC; -- most recent version

DESCRIBE HISTORY fact_apj_sales;

SELECT * FROM fact_apj_sales VERSION AS OF 1 ORDER BY ts DESC; -- data before new row inserted


