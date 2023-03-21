--------------------
/* replace your_database_name in the following command by your database name 
get your databse name by running the first cell of the prep-notebook 
*/
--------------------
USE your_database_name;
--------------------


SHOW TABLES;

SELECT  * FROM dim_store_locations;

DESCRIBE DETAIL dim_store_locations;

DESCRIBE HISTORY dim_store_locations;

DESCRIBE EXTENDED dim_store_locations;



INSERT () VALUES INTO dim_store_locations;

SELECT * FROM dim_store_locations; -- most recent version

DESCRIBE HISTORY dim_store_locations;

SELECT * FROM dim_store_locations AS VERSION 1; -- data before new row inserted
