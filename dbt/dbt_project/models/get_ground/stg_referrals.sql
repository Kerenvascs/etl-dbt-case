{{
    config(
        materialized='incremental',
        unique_key='model_pk_id'
    )
}}

WITH 
    referrals_corrected AS (
        SELECT
            id,
            company_id,
            partner_id,
            consultant_id,
            status,
            is_outbound,

            -- Formating strings to normalize unix nano, in order to apply formula to timestamp conversion.        
            CASE
                WHEN LENGTH(created_at) = 20 THEN CONCAT(REPLACE(SPLIT_PART(created_at, 'E+', 1),'.', ''), '0')
                WHEN LENGTH(created_at) = 19 THEN CONCAT(REPLACE(SPLIT_PART(created_at, 'E+', 1),'.', ''), '00')
                WHEN LENGTH(created_at) = 18 THEN CONCAT(REPLACE(SPLIT_PART(created_at, 'E+', 1),'.', ''), '000')
                ELSE REPLACE(SPLIT_PART(created_at, 'E+', 1),'.', '')
            END AS created_at,
            
            CASE
                WHEN LENGTH(updated_at) = 20 THEN CONCAT(REPLACE(SPLIT_PART(updated_at, 'E+', 1),'.', ''), '0')
                WHEN LENGTH(updated_at) = 19 THEN CONCAT(REPLACE(SPLIT_PART(updated_at, 'E+', 1),'.', ''), '00')
                WHEN LENGTH(updated_at) = 18 THEN CONCAT(REPLACE(SPLIT_PART(updated_at, 'E+', 1),'.', ''), '000')
                ELSE REPLACE(SPLIT_PART(updated_at, 'E+', 1),'.', '')
            END AS updated_at
        FROM referrals),

    referrals_final AS ( 
        SELECT
            MD5(id || created_at) AS model_pk_id,
            id,
            company_id,
            partner_id,
            consultant_id,
            status,
            is_outbound,
            to_timestamp(created_at::double precision / 100000) AS referral_creation_date,
            to_timestamp(updated_at::double precision / 100000) AS referral_update_date,
            current_timestamp AS model_updated_dt              
        FROM referrals_corrected)
    
    SELECT * FROM referrals_final

{% if is_incremental() %}

-- This filter will only be applied on an incremental run
    WHERE referral_update_date > (SELECT max(referral_update_date) FROM {{this}})

{% endif %}

-- NOTES
-- Add model primary key
-- The model_pk_id is the Primary Key used for the unique_key tag in the header.
--The MD5 function creates a hash value based on the string, converting model_pk_id to a 32-character string that is a text representation 
-- of the hexadecimal value of a 128-bit checksum.
--All incremental table MUST have a correct model_pk_id attribute. This is necessary to guarantee the removal of duplications.


-- Columns for date tracking (pre requisite of incremental tables):
-- referral_update_date: is a datetime type, that will be used as our reference to what is new data.   
-- current_timestamp AS **model_updated_dt**: It is an log attribute, registering when a given line was inserted in the table. 
-- The recommendation is to use current_timestamp to get the datetime value.