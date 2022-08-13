{{
    config(
        materialized='incremental',
        -- unique_key='date_day'
    )
}}

WITH 
    partners_corrected AS (
        SELECT
            id,
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


    SELECT
        id,
        to_timestamp(created_at::double precision / 100000) AS partners_creation_date,
        to_timestamp(updated_at::double precision / 100000) AS partners_update_date,    
        partner_type,
        lead_sales_contact
        
    FROM partners_corrected;   
