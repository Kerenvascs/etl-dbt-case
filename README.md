# GetGround BI Task

Welcome to GetGround's Data Task! We hope you find it fun.

We've included some data from SQL tables in CSV format. The goal will be to insert this data into a SQL database on your local machine; run some SQL queries and analysis; document, explain and visualize your response to the questions asked.

## The Data

GetGround currently has end-customers referred to us by partners, such as lettings agents and mortgage brokers. The customer then signs up for our service, and we pay the partner a small commission per referrals. Referrals are on a company level: a customer who signs up for five companies counts as five referrals. Five customers in one company count as one referral.

Partners each have consultants, such as Joe Smith working at Lettings Agent A. The referrals are attributed to the specific consultant at a partner.

The data tables provided are as follows:

```
partners
	id
	created_at
	updated_at
	partner_type
  lead_sales_contact
```

```
sales people
  name
  country
```

```
referrals
	id
	created_at
	updated_at
  company_id
	partner_id
	consultant_id
	status
	is_outbound
```


For referrals, the `updated_at` field essentially says when the status went from `pending` to either `disinterested` or `successful`. Timestamps are in Unix Nano format.

`is_outbound` is `true` when we refer a customer to a partner, i.e. "upsell". In this case we send them the customer, and they pay _us_ a commission. We haven't done this very thoroughly yet, so most referrals are inbound.

Our sales people work in a "key account" model. Referrals come from partners, and a sales person typically manages partner accounts.

We currently have sales people in the UK, Singapore and Hong Kong.

## Questions and Exercises

1. Please insert the data provided as CSV into tables in an SQL database. Please include SQL queries used throughout the assignment.

2. Use dbt to pre-precess the data and output dbt models for analysis. Include appropriate data quality tests and documentation.

3. Analyse the data using SQL. Be sure to include your investigative thought process, findings, limitations, and assumptions.

4. Based on your analysis, how would you reccomend GG improve the quality of the analyses we can deliver.

## Environment configuration
### Anaconda installation
https://docs.anaconda.com/anaconda/install/linux/

### Conda environment
```
conda create -n test-dbt 
conda activate test-dbt 
pip install -r requirements.txt
```

## Local execution of Postgres
```
echo 'installing docker' 
sudo apt-get remove docker docker-engine docker.io
sudo apt install docker.io -y
sudo systemctl start docker
sudo systemctl enable docker
docker --version

chmod 777 /var/run/docker.sock
docker run hello-world

echo 'installing docker-compose' 
sudo curl -L "https://github.com/docker/compose/releases/download/1.24.0/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose
sudo chmod +x /usr/local/bin/docker-compose
docker-compose --version

cd /home/developer/Desktop/testes/tech-tests/docker-compose.yml
docker compose up
```

## Installation of dbt
```
pip install dbt-postgres
dbt --version #1.2.0 - Up to date!

dbt init dbt_project
# Config profile yml to sync with dbt_project yml according to the adapter, in our case is postgres.
# then run dbt_debug to check connection
```

