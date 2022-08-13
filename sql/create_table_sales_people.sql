CREATE TABLE sales_people (
name VARCHAR(50), 
country VARCHAR(50)
)

COPY sales_people(name, country)
FROM '/tmp/sales_people.csv'
DELIMITER ','
CSV HEADER;
