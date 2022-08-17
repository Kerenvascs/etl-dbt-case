{{
    config(
        materialized='view',
    )
}}

WITH 
    partners AS (

        SELECT * FROM {{ ref('stg_partners') }}

    ),

    referrals AS (

        SELECT * FROM {{ ref('stg_referrals') }}
    ),

    sales_people AS (

        SELECT * FROM {{ ref('stg_sales_people') }}
    )


    SELECT 
        -- columns from model Partners
        partners.id AS partners_id,
        partners.partner_type,
        partners.lead_sales_contact,
        partners.partners_creation_date,
        partners.partners_update_date,
        -- columns from model Referrals
        referrals.id AS referral_id,
        referrals.company_id,
        referrals.consultant_id,
        referrals.status,
        referrals.is_outbound,
        referrals.referral_creation_date,
        referrals.referral_update_date,
        -- Columns from  model Sales people
        sales_people.name AS sales_contact_name,
        sales_people.country

    FROM partners
    LEFT JOIN referrals ON partners.id = referrals.partner_id
    LEFT JOIN sales_people ON partners.lead_sales_contact = sales_people.name
    