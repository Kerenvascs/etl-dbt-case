CREATE TABLE referrals(
  id SERIAL,
  created_at VARCHAR(50),
  updated_at VARCHAR(50),
  company_id INT,
  partner_id INT,
  consultant_id INT,
  status VARCHAR(20),
  is_outbound INT,
  PRIMARY KEY (id)
  )

COPY referrals(id, created_at, updated_at, company_id, partner_id, consultant_id, status, is_outbound)
FROM '/tmp/referrals.csv'
DELIMITER ','
CSV HEADER;