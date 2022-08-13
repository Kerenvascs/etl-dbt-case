
{{ config(materialized='table') }}

WITH 
    sales_people AS (
        SELECT
            name AS partner_name, 
            country
        FROM sales_people
    )

SELECT * 
FROM sales_people