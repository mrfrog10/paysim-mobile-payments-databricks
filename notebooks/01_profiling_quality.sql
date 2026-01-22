-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 01 · Data profiling and quality checks (SQL)
-- MAGIC
-- MAGIC Profiles `bronze_transactions` and records every check in `paysim.dq_results`, so the
-- MAGIC dashboard and README can show what passed, what is a warning and why.
-- MAGIC
-- MAGIC **Time:** `step` is one simulated hour (1–743, about 31 days). This project uses
-- MAGIC `hour_index = step - 1`, so `day = hour_index DIV 24 + 1` and `hour_of_day = hour_index % 24`.

-- COMMAND ----------

USE CATALOG workspace;
USE SCHEMA paysim;

-- COMMAND ----------

-- MAGIC %md ## 1. Shape and coverage

-- COMMAND ----------

SELECT COUNT(*)                  AS n_rows,
       MIN(step)                 AS min_step,
       MAX(step)                 AS max_step,
       COUNT(DISTINCT nameOrig)  AS distinct_senders,
       COUNT(DISTINCT nameDest)  AS distinct_receivers,
       SUM(isFraud)              AS fraud_rows,
       SUM(isFlaggedFraud)       AS flagged_rows
FROM bronze_transactions;

-- COMMAND ----------

-- Null count per column
SELECT
  COUNT_IF(step IS NULL)           AS step_nulls,
  COUNT_IF(type IS NULL)           AS type_nulls,
  COUNT_IF(amount IS NULL)         AS amount_nulls,
  COUNT_IF(nameOrig IS NULL)       AS nameOrig_nulls,
  COUNT_IF(nameDest IS NULL)       AS nameDest_nulls,
  COUNT_IF(oldbalanceOrg IS NULL)  AS oldbalanceOrg_nulls,
  COUNT_IF(isFraud IS NULL)        AS isFraud_nulls
FROM bronze_transactions;

-- COMMAND ----------

-- Transaction types: count, value and fraud
SELECT type,
       COUNT(*)                                             AS n,
       ROUND(100 * COUNT(*) / SUM(COUNT(*)) OVER (), 2)     AS pct_of_txns,
       ROUND(SUM(amount) / 1e9, 1)                          AS value_bn,
       ROUND(100 * SUM(amount) / SUM(SUM(amount)) OVER (), 1) AS pct_of_value,
       ROUND(PERCENTILE_APPROX(amount, 0.5), 0)             AS median_amount,
       SUM(isFraud)                                         AS fraud_n,
       ROUND(100 * AVG(isFraud), 3)                         AS fraud_rate_pct
FROM bronze_transactions
GROUP BY type
ORDER BY n DESC;

-- COMMAND ----------

-- Volume per simulated day: shows the activity drop after day 17
SELECT (step - 1) DIV 24 + 1 AS day,
       COUNT(*)              AS n,
       SUM(isFraud)          AS fraud_n
FROM bronze_transactions
GROUP BY 1
ORDER BY 1;

-- COMMAND ----------

-- MAGIC %md ## 2. Quality checks → `dq_results`
-- MAGIC Each check has an expectation, the observed failing rows and a decision (keep / exclude / flag).

-- COMMAND ----------

CREATE OR REPLACE TABLE dq_results AS
WITH b AS (SELECT * FROM bronze_transactions),
checks AS (
  SELECT 'row_count'               AS check_name, 'rows = 6,362,620 (Kaggle file)'        AS expectation, ABS(COUNT(*) - 6362620) AS failing_rows, 'blocker if > 0'                           AS decision FROM b
  UNION ALL
  SELECT 'no_nulls',              'no NULL in any key column',
         COUNT_IF(step IS NULL OR type IS NULL OR amount IS NULL OR nameOrig IS NULL OR nameDest IS NULL OR isFraud IS NULL), 'blocker if > 0' FROM b
  UNION ALL
  SELECT 'exact_duplicates',      'no fully duplicated rows',
         COUNT(*) - (SELECT COUNT(*) FROM (SELECT DISTINCT * FROM b)), 'drop duplicates if > 0' FROM b
  UNION ALL
  SELECT 'valid_type',            'type in 5 known values',
         COUNT_IF(type NOT IN ('PAYMENT','TRANSFER','CASH_OUT','CASH_IN','DEBIT')), 'blocker if > 0' FROM b
  UNION ALL
  SELECT 'positive_amount',       'amount > 0',
         COUNT_IF(amount <= 0), 'exclude from usage metrics, keep for fraud (all are fraud rows)' FROM b
  UNION ALL
  SELECT 'step_range',            'step between 1 and 744',
         COUNT_IF(step < 1 OR step > 744), 'blocker if > 0' FROM b
  UNION ALL
  SELECT 'id_prefix',             'nameOrig starts C; nameDest starts C or M',
         COUNT_IF(nameOrig NOT LIKE 'C%' OR NOT (nameDest LIKE 'C%' OR nameDest LIKE 'M%')), 'blocker if > 0' FROM b
  UNION ALL
  SELECT 'merchant_only_payment', 'M receivers only on PAYMENT',
         COUNT_IF(nameDest LIKE 'M%' AND type <> 'PAYMENT'), 'flag' FROM b
  UNION ALL
  SELECT 'orig_balance_math',     'oldbalanceOrg - amount = newbalanceOrig (TRANSFER/CASH_OUT, 1 cent tolerance)',
         COUNT_IF(type IN ('TRANSFER','CASH_OUT') AND ABS(oldbalanceOrg - amount - newbalanceOrig) > 0.01),
         'warning: known simulator behaviour; do not use balances as ground truth' FROM b
  UNION ALL
  SELECT 'dest_balance_zero',     'customer receiver balances not both 0 when amount > 0',
         COUNT_IF(nameDest LIKE 'C%' AND amount > 0 AND oldbalanceDest = 0 AND newbalanceDest = 0),
         'warning: missing receiver balances' FROM b
  UNION ALL
  SELECT 'flag_matches_fraud',    'isFlaggedFraud = 1 only when isFraud = 1',
         COUNT_IF(isFlaggedFraud = 1 AND isFraud = 0), 'blocker if > 0' FROM b
  UNION ALL
  SELECT 'fraud_only_two_types',  'fraud only in TRANSFER and CASH_OUT',
         COUNT_IF(isFraud = 1 AND type NOT IN ('TRANSFER','CASH_OUT')), 'flag' FROM b
)
SELECT *,
       CASE WHEN failing_rows = 0 THEN 'PASS'
            WHEN decision LIKE 'blocker%' THEN 'FAIL'
            ELSE 'WARN' END AS status,
       current_timestamp() AS checked_at
FROM checks;

SELECT * FROM dq_results ORDER BY status, check_name;

-- COMMAND ----------

-- MAGIC %md ## 3. Checks that need a closer look

-- COMMAND ----------

-- The 16 non-positive amounts: all are zero-amount fraud rows at the end of a drain
SELECT type, isFraud, COUNT(*) AS n, MIN(amount), MAX(amount)
FROM bronze_transactions WHERE amount <= 0 GROUP BY ALL;

-- COMMAND ----------

-- The dataset description says isFlaggedFraud fires on transfers above 200,000.
-- Test the claim: how many such transfers exist, and how many are flagged?
SELECT COUNT_IF(type = 'TRANSFER' AND amount > 200000)                         AS transfers_over_200k,
       COUNT_IF(type = 'TRANSFER' AND amount > 200000 AND isFlaggedFraud = 1)  AS of_which_flagged,
       COUNT_IF(isFlaggedFraud = 1)                                            AS flagged_total,
       SUM(isFraud)                                                            AS fraud_total
FROM bronze_transactions;

-- COMMAND ----------

-- How often does a sender come back? (basis for choosing the retention definition in notebook 02)
SELECT n_txns, COUNT(*) AS senders, ROUND(100 * COUNT(*) / SUM(COUNT(*)) OVER (), 3) AS pct
FROM (SELECT nameOrig, COUNT(*) AS n_txns FROM bronze_transactions GROUP BY nameOrig)
GROUP BY n_txns ORDER BY n_txns;

-- COMMAND ----------

-- MAGIC %md ## 4. Silver table
-- MAGIC Adds the time columns and keeps every row; `is_valid_amount` lets usage metrics skip the 16 zero-amount rows.

-- COMMAND ----------

CREATE OR REPLACE TABLE silver_transactions AS
SELECT *,
       step - 1                    AS hour_index,
       (step - 1) DIV 24 + 1       AS day,
       (step - 1) % 24             AS hour_of_day,
       (step - 1) DIV 168 + 1      AS week,
       amount > 0                  AS is_valid_amount,
       substr(nameDest, 1, 1) = 'M' AS to_merchant
FROM bronze_transactions;

SELECT COUNT(*) FROM silver_transactions;
