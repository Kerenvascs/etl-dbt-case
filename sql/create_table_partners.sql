CREATE TABLE partners (
  id SERIAL,
  created_at VARCHAR(50),
  updated_at VARCHAR(50),
  partner_type VARCHAR(50),
  lead_sales_contact VARCHAR(50),
  PRIMARY KEY (id)
)

COPY partners(id, created_at, updated_at, partner_type, lead_sales_contact)
FROM '/tmp/partners.csv'
DELIMITER ','
CSV HEADER;


-- To transform into model on dbt
--select 1599105249006
select to_timestamp(160247808477227::double precision / 100000)