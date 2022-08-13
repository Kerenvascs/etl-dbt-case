{{
    config(
        materialized='incremental',
        unique_key='date_day'
    )
}}

WITH 
    partners_corrected AS (
        SELECT
            id AS partner_id
            partner_type,
            lead_sales_contact,

            -- Formating strings to normalize unix nano, in order to apply formula to timestamp conversion.        
            CASE
                WHEN LENGTH(created_at) = 20
                THEN CONCAT(REPLACE(SPLIT_PART(created_at, 'E+', 1),'.', ''), '0')
                WHEN LENGTH(created_at) = 19
                THEN CONCAT(REPLACE(SPLIT_PART(created_at, 'E+', 1),'.', ''), '00')
                WHEN LENGTH(created_at) = 18
                THEN CONCAT(REPLACE(SPLIT_PART(created_at, 'E+', 1),'.', ''), '000')
                ELSE REPLACE(SPLIT_PART(created_at, 'E+', 1),'.', '')
            END AS created_at,
            
            CASE
                WHEN LENGTH(updated_at) = 20
                THEN CONCAT(REPLACE(SPLIT_PART(updated_at, 'E+', 1),'.', ''), '0')
                WHEN LENGTH(updated_at) = 19
                THEN CONCAT(REPLACE(SPLIT_PART(updated_at, 'E+', 1),'.', ''), '00')
                WHEN LENGTH(updated_at) = 18
                THEN CONCAT(REPLACE(SPLIT_PART(updated_at, 'E+', 1),'.', ''), '000')
                ELSE REPLACE(SPLIT_PART(updated_at, 'E+', 1),'.', '')
            END AS updated_at
           
        FROM partners)
        

    referrals_corrected AS (
        SELECT
            id AS referral_id,
            company_id,
            partner_id,
            consultant_id,
            status,
            is_outbound

            -- Formating strings to normalize unix nano, in order to apply formula to timestamp conversion.        
            CASE
                WHEN LENGTH(created_at) = 20
                THEN CONCAT(REPLACE(SPLIT_PART(created_at, 'E+', 1),'.', ''), '0')
                WHEN LENGTH(created_at) = 19
                THEN CONCAT(REPLACE(SPLIT_PART(created_at, 'E+', 1),'.', ''), '00')
                WHEN LENGTH(created_at) = 18
                THEN CONCAT(REPLACE(SPLIT_PART(created_at, 'E+', 1),'.', ''), '000')
                ELSE REPLACE(SPLIT_PART(created_at, 'E+', 1),'.', '')
            END AS created_at,
            
            CASE
                WHEN LENGTH(updated_at) = 20
                THEN CONCAT(REPLACE(SPLIT_PART(updated_at, 'E+', 1),'.', ''), '0')
                WHEN LENGTH(updated_at) = 19
                THEN CONCAT(REPLACE(SPLIT_PART(updated_at, 'E+', 1),'.', ''), '00')
                WHEN LENGTH(updated_at) = 18
                THEN CONCAT(REPLACE(SPLIT_PART(updated_at, 'E+', 1),'.', ''), '000')
                ELSE REPLACE(SPLIT_PART(updated_at, 'E+', 1),'.', '')
            END AS updated_at

        FROM referrals)


    sales_people AS (
        SELECT
            name AS partner_name, 
            country
        FROM sales_people
    )

SELECT
    partners_corrected.partner_id,
    to_timestamp(partners_corrected.created_at::double precision / 100000) AS creation_date,
    to_timestamp(partners_corrected.updated_at::double precision / 100000) AS update_date,    
    partners_corrected.partner_type,
    partners_corrected.lead_sales_contact
    
FROM partners_corrected;   



--1.String format: 
--- To remove the points: SELECT REPLACE(created_at, '.', '')

--2.String split: 
----Remove zeros FROM E on
----https://w3resource.com/PostgreSQL/split_part-function.php
----split_part('ordno-#-orddt-#-ordamt', '-#-', 2);
----SELECT SPLIT_PART(created_at, 'E+', 1) AS created_str_corrected, created_at  FROM partners;

--3: convert to timestamp
--to_timestamp(created_at::double precision / 100000)

