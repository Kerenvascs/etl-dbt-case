{{
    config(
        materialized='incremental',
        unique_key='model_pk_id'
    )
}}

WITH 
    partners_corrected AS (
        SELECT
            MD5(id || created_at) AS model_pk_id,
            id,
            partner_type,
            NULLIF(lead_sales_contact, '0') AS lead_sales_contact,

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
           
        FROM partners),

    partners_final AS (
        SELECT
            model_pk_id,
            id, 
            partner_type,
            lead_sales_contact,
            to_timestamp(created_at::double precision / 100000) AS partners_creation_date,
            to_timestamp(updated_at::double precision / 100000) AS partners_update_date,
            current_timestamp AS model_updated_dt 
        FROM partners_corrected)
    
    SELECT * FROM partners_final
        
        
{% if is_incremental() %}

-- This filter will only be applied on an incremental run
    WHERE partners_update_date > (SELECT max(partners_update_date) FROM {{this}})

{% endif %}

-- The model_pk_id is the Primary Key used for the unique_key tag in the header.
--The MD5 function creates a hash value based on the string, converting model_pk_id to a 32-character string that is a text representation 
-- of the hexadecimal value of a 128-bit checksum.
--All incremental table MUST have a correct model_pk_id attribute. This is necessary to guarantee the removal of duplications