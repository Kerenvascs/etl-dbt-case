CREATE TABLE partners (
  id SERIAL,
  created_at VARCHAR(50),
  updated_at VARCHAR(50),
  partner_type VARCHAR(50),
  lead_sales_contact VARCHAR(255),
  PRIMARY KEY (id)
)

COPY partners(id, created_at, updated_at, partner_type, lead_sales_contact)
FROM '/tmp/partners.csv'
DELIMITER ','
CSV HEADER;
--select 1599105249006
select now
select FROM_UNIXTIME(1599105249006,'%a %b %d %H:%i:%s UTC %Y');
select to_timestamp(160247808477227::double precision / 100000), to_timestamp(1602478084772270::double precision / 1000000);
select to_timestamp(1602478084.77227::double) 