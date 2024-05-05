


-- Option 1 - passing workflow parameters 
SELECT * FROM identifier({{catalog}} || '.' || {{schema}}  || '.' || 'customer');

-- Option 2 - Using Variables and assigning workflow parameters to variables.
DECLARE OR REPLACE VARIABLE catalog_name STRING DEFAULT 'samples';
DECLARE OR REPLACE VARIABLE schema_name STRING DEFAULT 'tpch';

SET VAR catalog_name = {{catalog}};
SET VAR schema_name = {{schema}};


SELECT * FROM IDENTIFIER(catalog_name || '.' || schema_name  || '.' || 'customer');