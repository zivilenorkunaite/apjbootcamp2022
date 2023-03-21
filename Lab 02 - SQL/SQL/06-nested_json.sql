--------------------
/* replace your_database_name in the following command by your database name 
get your databse name by running the first cell of the prep-notebook 
*/
--------------------
USE your_database_name;
--------------------

SELECT * FROM store_data_json;

--Extract a top-level column
SELECT  raw:owner FROM store_data_json;

--Extract nested fields
SELECT raw:store.bicycle FROM store_data_json;

--escape characters
SELECT  raw:owner, raw:`fb:testid`, raw:`zip code`  FROM store_data_json;

--Extract values from arrays
-- Index elements
SELECT raw:store.fruit[0], raw:store.fruit[1] FROM store_data_json;

-- Extract subfields from arrays
SELECT raw:store.book[*].isbn FROM store_data_json;

