{{
    config(
        materialized='incremental',
        -- unique_key='date_day'
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

    SELECT
        id,
        company_id,
        partner_id,
        consultant_id,
        status,
        is_outbound,
        to_timestamp(created_at::double precision / 100000) AS referral_creation_date,
        to_timestamp(updated_at::double precision / 100000) AS referral_update_date,    
        lead_sales_contact
        
    FROM referrals_corrected;

    # 5. Final query to create table demanda on dbt

The SELECT command base will be the same as originally thought for a given model, with a few important additions.


