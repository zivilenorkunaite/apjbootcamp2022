--------------------
/* replace your_database_name in the following command by your database name 
get your databse name by running the first cell of the prep-notebook 
*/
--------------------
USE your_database_name;
--------------------

-- Literals
CREATE OR REPLACE FUNCTION blue()
    RETURN '0000FF';
  
SELECT blue();


-- encapsulating expressions
CREATE OR REPLACE FUNCTION to_hex(x INT )
  RETURN lpad(hex(least(greatest(0, x), 255)), 2, 0);


SELECT to_hex(id) FROM range(2);


-- Look up table
CREATE OR REPLACE TABLE colors(rgb STRING NOT NULL, name STRING NOT NULL);
INSERT INTO colors VALUES
  ('FF00FF', 'magenta'),
  ('FF0080', 'rose'),
  ('BFFF00', 'lime'),
  ('7DF9FF', 'electric blue');

CREATE OR REPLACE FUNCTION
from_rgb_scalar(rgb STRING ) 
RETURN SELECT FIRST(name) FROM colors WHERE colors.rgb = from_rgb_scalar.rgb;

SELECT from_rgb_scalar(rgb) FROM
VALUES ('7DF9FF'),  ('BFFF00') AS codes(rgb);

