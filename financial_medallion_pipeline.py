# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Title
# MAGIC %md
# MAGIC # Financial Data Medallion Pipeline
# MAGIC
# MAGIC Bronze → Silver → Gold pipeline processing synthetic financial data from `/Volumes/db2databricksmigration/kanfatma_agentic_landing/raw_data` into managed Delta tables under catalog `db2databricksmigration`.

# COMMAND ----------

# DBTITLE 1,Create medallion schemas
CATALOG = "db2databricksmigration"
VOLUME_PATH = "/Volumes/db2databricksmigration/kanfatma_agentic_landing/raw_data"

for schema_name, comment in [
    ("kanfatma_bronze", "Bronze layer - raw ingestion from CSV"),
    ("kanfatma_silver", "Silver layer - cleaned and conformed"),
    ("kanfatma_gold",   "Gold layer - business aggregates"),
]:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{schema_name} COMMENT '{comment}'")
    print(f"✓ {CATALOG}.{schema_name}")

# COMMAND ----------

# DBTITLE 1,Bronze Layer
# MAGIC %md
# MAGIC ## Bronze Layer
# MAGIC
# MAGIC Raw ingestion from CSV files with audit metadata columns (`_ingested_at`, `_source_file`). No transformations applied.

# COMMAND ----------

# DBTITLE 1,Bronze tables
# MAGIC %sql
# MAGIC -- Reference tables
# MAGIC CREATE OR REPLACE TABLE db2databricksmigration.kanfatma_bronze.bronze_branches
# MAGIC AS SELECT *, current_timestamp() AS _ingested_at, _metadata.file_path AS _source_file
# MAGIC FROM read_files('/Volumes/db2databricksmigration/kanfatma_agentic_landing/raw_data/branches',
# MAGIC   format => 'csv', header => 'true', inferSchema => 'true');
# MAGIC
# MAGIC CREATE OR REPLACE TABLE db2databricksmigration.kanfatma_bronze.bronze_products
# MAGIC AS SELECT *, current_timestamp() AS _ingested_at, _metadata.file_path AS _source_file
# MAGIC FROM read_files('/Volumes/db2databricksmigration/kanfatma_agentic_landing/raw_data/products',
# MAGIC   format => 'csv', header => 'true', inferSchema => 'true');
# MAGIC
# MAGIC CREATE OR REPLACE TABLE db2databricksmigration.kanfatma_bronze.bronze_date_dimensions
# MAGIC AS SELECT *, current_timestamp() AS _ingested_at, _metadata.file_path AS _source_file
# MAGIC FROM read_files('/Volumes/db2databricksmigration/kanfatma_agentic_landing/raw_data/date_dimensions',
# MAGIC   format => 'csv', header => 'true', inferSchema => 'true');
# MAGIC
# MAGIC -- Fact tables
# MAGIC CREATE OR REPLACE TABLE db2databricksmigration.kanfatma_bronze.bronze_customers
# MAGIC AS SELECT *, current_timestamp() AS _ingested_at, _metadata.file_path AS _source_file
# MAGIC FROM read_files('/Volumes/db2databricksmigration/kanfatma_agentic_landing/raw_data/customers',
# MAGIC   format => 'csv', header => 'true', inferSchema => 'true');
# MAGIC
# MAGIC CREATE OR REPLACE TABLE db2databricksmigration.kanfatma_bronze.bronze_accounts
# MAGIC AS SELECT *, current_timestamp() AS _ingested_at, _metadata.file_path AS _source_file
# MAGIC FROM read_files('/Volumes/db2databricksmigration/kanfatma_agentic_landing/raw_data/accounts',
# MAGIC   format => 'csv', header => 'true', inferSchema => 'true');
# MAGIC
# MAGIC CREATE OR REPLACE TABLE db2databricksmigration.kanfatma_bronze.bronze_transactions
# MAGIC AS SELECT *, current_timestamp() AS _ingested_at, _metadata.file_path AS _source_file
# MAGIC FROM read_files('/Volumes/db2databricksmigration/kanfatma_agentic_landing/raw_data/transactions',
# MAGIC   format => 'csv', header => 'true', inferSchema => 'true');

# COMMAND ----------

# DBTITLE 1,Silver Layer
# MAGIC %md
# MAGIC ## Silver Layer
# MAGIC
# MAGIC Cleaned and conformed data: enforced types, `Y/N` flags cast to booleans, computed columns added.

# COMMAND ----------

# DBTITLE 1,Silver branches
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE db2databricksmigration.kanfatma_silver.silver_branches AS
# MAGIC SELECT
# MAGIC   branch_id,
# MAGIC   branch_name,
# MAGIC   branch_type,
# MAGIC   city,
# MAGIC   region,
# MAGIC   country,
# MAGIC   postal_code,
# MAGIC   phone,
# MAGIC   manager_name,
# MAGIC   CAST(num_employees AS INT)       AS num_employees,
# MAGIC   CAST(opened_date AS DATE)        AS opened_date,
# MAGIC   is_24h_atm = 'Y'                AS is_24h_atm,
# MAGIC   has_safe_deposit = 'Y'           AS has_safe_deposit,
# MAGIC   has_business_services = 'Y'      AS has_business_services,
# MAGIC   status,
# MAGIC   _ingested_at
# MAGIC FROM db2databricksmigration.kanfatma_bronze.bronze_branches

# COMMAND ----------

# DBTITLE 1,Silver products
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE db2databricksmigration.kanfatma_silver.silver_products AS
# MAGIC SELECT
# MAGIC   product_id,
# MAGIC   product_name,
# MAGIC   product_category,
# MAGIC   CAST(interest_rate_pct AS DECIMAL(5,2)) AS interest_rate_pct,
# MAGIC   CAST(apr_pct AS DECIMAL(5,2))           AS apr_pct,
# MAGIC   CAST(min_balance AS INT)                AS min_balance,
# MAGIC   CAST(monthly_fee AS INT)                AS monthly_fee,
# MAGIC   CAST(annual_fee AS INT)                 AS annual_fee,
# MAGIC   CAST(reward_rate_pct AS DECIMAL(5,2))   AS reward_rate_pct,
# MAGIC   CAST(term_months AS INT)                AS term_months,
# MAGIC   fdic_insured = 'Y'                      AS is_fdic_insured,
# MAGIC   CAST(min_credit_score AS INT)           AS min_credit_score,
# MAGIC   CAST(launch_date AS DATE)               AS launch_date,
# MAGIC   status,
# MAGIC   product_type,
# MAGIC   _ingested_at
# MAGIC FROM db2databricksmigration.kanfatma_bronze.bronze_products

# COMMAND ----------

# DBTITLE 1,Silver date dimensions
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE db2databricksmigration.kanfatma_silver.silver_date_dimensions AS
# MAGIC SELECT
# MAGIC   CAST(full_date AS DATE)                  AS full_date,
# MAGIC   CAST(date_key AS INT)                    AS date_key,
# MAGIC   CAST(calendar_year AS INT)               AS calendar_year,
# MAGIC   CAST(calendar_quarter AS INT)            AS calendar_quarter,
# MAGIC   calendar_quarter_name,
# MAGIC   CAST(month_number AS INT)                AS month_number,
# MAGIC   month_name,
# MAGIC   month_short,
# MAGIC   year_month,
# MAGIC   CAST(week_of_year AS INT)                AS week_of_year,
# MAGIC   CAST(day_of_month AS INT)                AS day_of_month,
# MAGIC   CAST(day_of_week_number AS INT)          AS day_of_week_number,
# MAGIC   day_of_week_name,
# MAGIC   day_of_week_short,
# MAGIC   CAST(fiscal_year AS INT)                 AS fiscal_year,
# MAGIC   CAST(fiscal_quarter AS INT)              AS fiscal_quarter,
# MAGIC   is_weekend = 'Y'                         AS is_weekend,
# MAGIC   is_month_end = 'Y'                       AS is_month_end,
# MAGIC   is_year_end = 'Y'                        AS is_year_end,
# MAGIC   _ingested_at
# MAGIC FROM db2databricksmigration.kanfatma_bronze.bronze_date_dimensions

# COMMAND ----------

# DBTITLE 1,Silver customers
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE db2databricksmigration.kanfatma_silver.silver_customers AS
# MAGIC SELECT
# MAGIC   customer_id,
# MAGIC   first_name,
# MAGIC   last_name,
# MAGIC   full_name,
# MAGIC   email,
# MAGIC   phone,
# MAGIC   CAST(date_of_birth AS DATE)              AS date_of_birth,
# MAGIC   CAST(FLOOR(DATEDIFF(CURRENT_DATE(), date_of_birth) / 365.25) AS INT) AS age,
# MAGIC   gender,
# MAGIC   street_address,
# MAGIC   city,
# MAGIC   postal_code,
# MAGIC   country,
# MAGIC   customer_segment,
# MAGIC   CAST(credit_score AS INT)                AS credit_score,
# MAGIC   CAST(annual_income AS DECIMAL(12,2))     AS annual_income,
# MAGIC   employment_status,
# MAGIC   primary_branch_id,
# MAGIC   CAST(customer_since AS DATE)             AS customer_since,
# MAGIC   kyc_verified = 'Y'                       AS is_kyc_verified,
# MAGIC   marketing_consent = 'Y'                  AS has_marketing_consent,
# MAGIC   digital_banking_enrolled = 'Y'           AS is_digital_banking_enrolled,
# MAGIC   status,
# MAGIC   _ingested_at
# MAGIC FROM db2databricksmigration.kanfatma_bronze.bronze_customers

# COMMAND ----------

# DBTITLE 1,Silver accounts
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE db2databricksmigration.kanfatma_silver.silver_accounts AS
# MAGIC SELECT
# MAGIC   account_id,
# MAGIC   account_number,
# MAGIC   customer_id,
# MAGIC   product_id,
# MAGIC   account_type,
# MAGIC   currency,
# MAGIC   CAST(current_balance AS DECIMAL(14,2))   AS current_balance,
# MAGIC   CAST(available_balance AS DECIMAL(14,2)) AS available_balance,
# MAGIC   CAST(credit_limit AS INT)                AS credit_limit,
# MAGIC   CAST(interest_rate_pct AS DECIMAL(5,2))  AS interest_rate_pct,
# MAGIC   CAST(opened_date AS DATE)                AS opened_date,
# MAGIC   CAST(last_activity_date AS DATE)         AS last_activity_date,
# MAGIC   branch_id,
# MAGIC   is_primary = 'Y'                         AS is_primary,
# MAGIC   overdraft_protection = 'Y'               AS has_overdraft_protection,
# MAGIC   paperless_statements = 'Y'               AS has_paperless_statements,
# MAGIC   status,
# MAGIC   _ingested_at
# MAGIC FROM db2databricksmigration.kanfatma_bronze.bronze_accounts

# COMMAND ----------

# DBTITLE 1,Silver transactions
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE db2databricksmigration.kanfatma_silver.silver_transactions AS
# MAGIC SELECT
# MAGIC   transaction_id,
# MAGIC   account_id,
# MAGIC   CAST(transaction_date AS DATE)           AS transaction_date,
# MAGIC   CAST(
# MAGIC     CONCAT(CAST(transaction_date AS STRING), 'T',
# MAGIC            DATE_FORMAT(transaction_time, 'HH:mm:ss'))
# MAGIC     AS TIMESTAMP
# MAGIC   )                                        AS transaction_timestamp,
# MAGIC   transaction_type,
# MAGIC   CAST(amount AS DECIMAL(12,2))            AS amount,
# MAGIC   currency,
# MAGIC   is_credit = 'Y'                          AS is_credit,
# MAGIC   CAST(running_balance AS DECIMAL(14,2))   AS running_balance,
# MAGIC   merchant_name,
# MAGIC   CAST(merchant_category_code AS STRING)   AS merchant_category_code,
# MAGIC   channel,
# MAGIC   branch_id,
# MAGIC   reference_number,
# MAGIC   counterparty_account,
# MAGIC   is_recurring = 'Y'                       AS is_recurring,
# MAGIC   is_international = 'Y'                   AS is_international,
# MAGIC   fraud_flag,
# MAGIC   status,
# MAGIC   description,
# MAGIC   _ingested_at
# MAGIC FROM db2databricksmigration.kanfatma_bronze.bronze_transactions

# COMMAND ----------

# DBTITLE 1,Gold Layer
# MAGIC %md
# MAGIC ## Gold Layer
# MAGIC
# MAGIC Business-level aggregates and analytics-ready tables built from Silver.

# COMMAND ----------

# DBTITLE 1,Gold customer 360
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE db2databricksmigration.kanfatma_gold.gold_customer_360 AS
# MAGIC -- CTE 1: pre-aggregate transactions to account grain (500K → 75K rows)
# MAGIC WITH account_txn_metrics AS (
# MAGIC   SELECT
# MAGIC     account_id,
# MAGIC     COUNT(*)                                             AS txn_count,
# MAGIC     SUM(CASE WHEN is_credit THEN amount ELSE 0 END)      AS total_credits,
# MAGIC     SUM(CASE WHEN NOT is_credit THEN amount ELSE 0 END)  AS total_debits,
# MAGIC     MAX(transaction_date)                                 AS last_transaction_date
# MAGIC   FROM db2databricksmigration.kanfatma_silver.silver_transactions
# MAGIC   GROUP BY account_id
# MAGIC ),
# MAGIC -- CTE 2: join accounts with CTE 1 (1:1, no fan-out), then aggregate to customer grain (75K → 50K rows)
# MAGIC customer_metrics AS (
# MAGIC   SELECT
# MAGIC     a.customer_id,
# MAGIC     COUNT(*)                                                                     AS total_accounts,
# MAGIC     SUM(CASE WHEN a.status = 'Active' THEN 1 ELSE 0 END)                        AS active_accounts,
# MAGIC     SUM(a.current_balance)                                                       AS total_balance,
# MAGIC     SUM(CASE WHEN a.current_balance > 0 THEN a.current_balance ELSE 0 END)      AS total_assets,
# MAGIC     SUM(CASE WHEN a.current_balance < 0 THEN ABS(a.current_balance) ELSE 0 END) AS total_liabilities,
# MAGIC     COALESCE(SUM(t.txn_count), 0)                                                AS total_transactions,
# MAGIC     COALESCE(SUM(t.total_credits), 0)                                            AS total_credits,
# MAGIC     COALESCE(SUM(t.total_debits), 0)                                             AS total_debits,
# MAGIC     MAX(t.last_transaction_date)                                                 AS last_transaction_date
# MAGIC   FROM      db2databricksmigration.kanfatma_silver.silver_accounts a
# MAGIC   LEFT JOIN account_txn_metrics t ON a.account_id = t.account_id
# MAGIC   GROUP BY a.customer_id
# MAGIC )
# MAGIC -- Final: 1:1 join on customer_id — no GROUP BY, no fan-out
# MAGIC SELECT
# MAGIC   c.customer_id,
# MAGIC   c.full_name,
# MAGIC   c.email,
# MAGIC   c.customer_segment,
# MAGIC   c.credit_score,
# MAGIC   c.annual_income,
# MAGIC   c.employment_status,
# MAGIC   c.city,
# MAGIC   c.country,
# MAGIC   c.age,
# MAGIC   c.customer_since,
# MAGIC   c.is_kyc_verified,
# MAGIC   c.is_digital_banking_enrolled,
# MAGIC   c.status                                     AS customer_status,
# MAGIC   COALESCE(m.total_accounts, 0)                AS total_accounts,
# MAGIC   COALESCE(m.active_accounts, 0)               AS active_accounts,
# MAGIC   COALESCE(m.total_balance, 0)                 AS total_balance,
# MAGIC   COALESCE(m.total_assets, 0)                  AS total_assets,
# MAGIC   COALESCE(m.total_liabilities, 0)             AS total_liabilities,
# MAGIC   COALESCE(m.total_transactions, 0)            AS total_transactions,
# MAGIC   COALESCE(m.total_credits, 0)                 AS total_credits,
# MAGIC   COALESCE(m.total_debits, 0)                  AS total_debits,
# MAGIC   m.last_transaction_date
# MAGIC FROM      db2databricksmigration.kanfatma_silver.silver_customers c
# MAGIC LEFT JOIN customer_metrics m ON c.customer_id = m.customer_id

# COMMAND ----------

# DBTITLE 1,Gold monthly transaction summary
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE db2databricksmigration.kanfatma_gold.gold_monthly_transaction_summary AS
# MAGIC SELECT
# MAGIC   a.account_id,
# MAGIC   a.account_type,
# MAGIC   a.customer_id,
# MAGIC   d.calendar_year,
# MAGIC   d.month_number,
# MAGIC   d.month_name,
# MAGIC   d.year_month,
# MAGIC   COUNT(*)                                                             AS transaction_count,
# MAGIC   SUM(CASE WHEN t.is_credit THEN t.amount ELSE 0 END)                 AS total_credits,
# MAGIC   SUM(CASE WHEN NOT t.is_credit THEN t.amount ELSE 0 END)             AS total_debits,
# MAGIC   SUM(CASE WHEN t.is_credit THEN t.amount ELSE -t.amount END)         AS net_flow,
# MAGIC   ROUND(AVG(t.amount), 2)                                              AS avg_transaction_amount,
# MAGIC   MAX(t.amount)                                                        AS max_transaction_amount,
# MAGIC   COUNT(DISTINCT t.merchant_name)                                      AS unique_merchants,
# MAGIC   SUM(CASE WHEN t.fraud_flag != 'N' THEN 1 ELSE 0 END)               AS flagged_transactions
# MAGIC FROM      db2databricksmigration.kanfatma_silver.silver_transactions     t
# MAGIC JOIN      db2databricksmigration.kanfatma_silver.silver_accounts         a ON t.account_id      = a.account_id
# MAGIC JOIN      db2databricksmigration.kanfatma_silver.silver_date_dimensions  d ON t.transaction_date = d.full_date
# MAGIC GROUP BY ALL

# COMMAND ----------

# DBTITLE 1,Gold branch performance
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE db2databricksmigration.kanfatma_gold.gold_branch_performance AS
# MAGIC WITH customer_counts AS (
# MAGIC   SELECT primary_branch_id AS branch_id, COUNT(*) AS customer_count
# MAGIC   FROM   db2databricksmigration.kanfatma_silver.silver_customers
# MAGIC   GROUP BY primary_branch_id
# MAGIC ),
# MAGIC account_metrics AS (
# MAGIC   SELECT branch_id,
# MAGIC          COUNT(DISTINCT account_id)   AS account_count,
# MAGIC          SUM(current_balance)         AS total_deposits,
# MAGIC          ROUND(AVG(current_balance),2)AS avg_account_balance
# MAGIC   FROM   db2databricksmigration.kanfatma_silver.silver_accounts
# MAGIC   GROUP BY branch_id
# MAGIC ),
# MAGIC transaction_metrics AS (
# MAGIC   SELECT a.branch_id,
# MAGIC          COUNT(DISTINCT t.transaction_id) AS transaction_count,
# MAGIC          SUM(t.amount)                    AS transaction_volume
# MAGIC   FROM   db2databricksmigration.kanfatma_silver.silver_transactions t
# MAGIC   JOIN   db2databricksmigration.kanfatma_silver.silver_accounts     a ON t.account_id = a.account_id
# MAGIC   GROUP BY a.branch_id
# MAGIC )
# MAGIC SELECT
# MAGIC   b.branch_id,
# MAGIC   b.branch_name,
# MAGIC   b.branch_type,
# MAGIC   b.city,
# MAGIC   b.region,
# MAGIC   b.num_employees,
# MAGIC   b.status                                     AS branch_status,
# MAGIC   COALESCE(cc.customer_count, 0)               AS customer_count,
# MAGIC   COALESCE(am.account_count, 0)                AS account_count,
# MAGIC   COALESCE(am.total_deposits, 0)               AS total_deposits,
# MAGIC   COALESCE(am.avg_account_balance, 0)          AS avg_account_balance,
# MAGIC   COALESCE(tm.transaction_count, 0)            AS transaction_count,
# MAGIC   COALESCE(tm.transaction_volume, 0)           AS transaction_volume
# MAGIC FROM      db2databricksmigration.kanfatma_silver.silver_branches b
# MAGIC LEFT JOIN customer_counts     cc ON b.branch_id = cc.branch_id
# MAGIC LEFT JOIN account_metrics     am ON b.branch_id = am.branch_id
# MAGIC LEFT JOIN transaction_metrics tm ON b.branch_id = tm.branch_id

# COMMAND ----------

# DBTITLE 1,Gold product adoption
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE db2databricksmigration.kanfatma_gold.gold_product_adoption AS
# MAGIC SELECT
# MAGIC   p.product_id,
# MAGIC   p.product_name,
# MAGIC   p.product_category,
# MAGIC   p.product_type,
# MAGIC   p.interest_rate_pct,
# MAGIC   p.status                                                            AS product_status,
# MAGIC   COUNT(DISTINCT a.account_id)                                        AS total_accounts,
# MAGIC   COUNT(DISTINCT CASE WHEN a.status = 'Active' THEN a.account_id END) AS active_accounts,
# MAGIC   COUNT(DISTINCT a.customer_id)                                       AS unique_customers,
# MAGIC   COALESCE(SUM(a.current_balance), 0)                                 AS total_balance,
# MAGIC   COALESCE(ROUND(AVG(a.current_balance), 2), 0)                       AS avg_balance,
# MAGIC   MIN(a.opened_date)                                                  AS earliest_account_opened,
# MAGIC   MAX(a.opened_date)                                                  AS latest_account_opened
# MAGIC FROM      db2databricksmigration.kanfatma_silver.silver_products  p
# MAGIC LEFT JOIN db2databricksmigration.kanfatma_silver.silver_accounts  a ON p.product_id = a.product_id
# MAGIC GROUP BY ALL

# COMMAND ----------

# DBTITLE 1,Gold fraud summary
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE db2databricksmigration.kanfatma_gold.gold_fraud_summary AS
# MAGIC SELECT
# MAGIC   t.fraud_flag,
# MAGIC   t.transaction_type,
# MAGIC   t.channel,
# MAGIC   d.calendar_year,
# MAGIC   d.calendar_quarter,
# MAGIC   d.month_name,
# MAGIC   COUNT(*)                                                        AS transaction_count,
# MAGIC   ROUND(SUM(t.amount), 2)                                         AS total_amount,
# MAGIC   ROUND(AVG(t.amount), 2)                                         AS avg_amount,
# MAGIC   COUNT(DISTINCT t.account_id)                                    AS affected_accounts,
# MAGIC   COUNT(DISTINCT t.merchant_name)                                 AS unique_merchants,
# MAGIC   SUM(CASE WHEN t.is_international THEN 1 ELSE 0 END)            AS international_count
# MAGIC FROM      db2databricksmigration.kanfatma_silver.silver_transactions    t
# MAGIC JOIN      db2databricksmigration.kanfatma_silver.silver_date_dimensions d ON t.transaction_date = d.full_date
# MAGIC WHERE     t.fraud_flag != 'N'
# MAGIC GROUP BY ALL

# COMMAND ----------

# DBTITLE 1,Pipeline Verification
# MAGIC %md
# MAGIC ## Pipeline Verification
# MAGIC
# MAGIC Row counts across all three medallion layers.

# COMMAND ----------

# DBTITLE 1,Verify row counts
# MAGIC %sql
# MAGIC SELECT 'Bronze' AS layer, 'bronze_branches'       AS table_name, COUNT(*) AS row_count FROM db2databricksmigration.kanfatma_bronze.bronze_branches
# MAGIC UNION ALL
# MAGIC SELECT 'Bronze', 'bronze_products',                  COUNT(*) FROM db2databricksmigration.kanfatma_bronze.bronze_products
# MAGIC UNION ALL
# MAGIC SELECT 'Bronze', 'bronze_date_dimensions',            COUNT(*) FROM db2databricksmigration.kanfatma_bronze.bronze_date_dimensions
# MAGIC UNION ALL
# MAGIC SELECT 'Bronze', 'bronze_customers',                  COUNT(*) FROM db2databricksmigration.kanfatma_bronze.bronze_customers
# MAGIC UNION ALL
# MAGIC SELECT 'Bronze', 'bronze_accounts',                   COUNT(*) FROM db2databricksmigration.kanfatma_bronze.bronze_accounts
# MAGIC UNION ALL
# MAGIC SELECT 'Bronze', 'bronze_transactions',                COUNT(*) FROM db2databricksmigration.kanfatma_bronze.bronze_transactions
# MAGIC UNION ALL
# MAGIC SELECT 'Silver', 'silver_branches',                   COUNT(*) FROM db2databricksmigration.kanfatma_silver.silver_branches
# MAGIC UNION ALL
# MAGIC SELECT 'Silver', 'silver_products',                   COUNT(*) FROM db2databricksmigration.kanfatma_silver.silver_products
# MAGIC UNION ALL
# MAGIC SELECT 'Silver', 'silver_date_dimensions',             COUNT(*) FROM db2databricksmigration.kanfatma_silver.silver_date_dimensions
# MAGIC UNION ALL
# MAGIC SELECT 'Silver', 'silver_customers',                   COUNT(*) FROM db2databricksmigration.kanfatma_silver.silver_customers
# MAGIC UNION ALL
# MAGIC SELECT 'Silver', 'silver_accounts',                    COUNT(*) FROM db2databricksmigration.kanfatma_silver.silver_accounts
# MAGIC UNION ALL
# MAGIC SELECT 'Silver', 'silver_transactions',                COUNT(*) FROM db2databricksmigration.kanfatma_silver.silver_transactions
# MAGIC UNION ALL
# MAGIC SELECT 'Gold',   'gold_customer_360',                  COUNT(*) FROM db2databricksmigration.kanfatma_gold.gold_customer_360
# MAGIC UNION ALL
# MAGIC SELECT 'Gold',   'gold_monthly_transaction_summary',   COUNT(*) FROM db2databricksmigration.kanfatma_gold.gold_monthly_transaction_summary
# MAGIC UNION ALL
# MAGIC SELECT 'Gold',   'gold_branch_performance',            COUNT(*) FROM db2databricksmigration.kanfatma_gold.gold_branch_performance
# MAGIC UNION ALL
# MAGIC SELECT 'Gold',   'gold_product_adoption',              COUNT(*) FROM db2databricksmigration.kanfatma_gold.gold_product_adoption
# MAGIC UNION ALL
# MAGIC SELECT 'Gold',   'gold_fraud_summary',                 COUNT(*) FROM db2databricksmigration.kanfatma_gold.gold_fraud_summary
# MAGIC ORDER BY layer, table_name