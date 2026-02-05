# Databricks notebook source
# MAGIC %md
# MAGIC # 02 · Product-usage analysis (PySpark)
# MAGIC
# MAGIC Questions a mobile-money product team would ask:
# MAGIC 1. How many transactions does a user make?
# MAGIC 2. What do people use the product for (transaction types, by count and by value)?
# MAGIC 3. Do new users come back? (weekly cohorts and retention)
# MAGIC 4. When is the product used (peak hours)?
# MAGIC 5. Where and when does fraud happen, and how well does the current flag catch it?
# MAGIC
# MAGIC Every result is saved as a `gold_*` table that feeds the dashboard.

# COMMAND ----------

from pyspark.sql import functions as F, Window as W

spark.sql("USE CATALOG workspace")
spark.sql("USE SCHEMA paysim")
tx = spark.table("silver_transactions")

def save(df, name):
    df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(name)
    return spark.table(name)

# COMMAND ----------

# MAGIC %md ## 1. Transactions per user
# MAGIC A "user" is a customer account (`C…`). Senders are counted from `nameOrig`.

# COMMAND ----------

per_sender = tx.groupBy("nameOrig").agg(F.count("*").alias("n_txns"))
txns_per_user = save(
    per_sender.groupBy("n_txns").agg(F.count("*").alias("users"))
      .withColumn("pct_users", F.round(100 * F.col("users") / F.sum("users").over(W.partitionBy()), 3))
      .orderBy("n_txns"),
    "gold_txns_per_user")
display(txns_per_user)

# COMMAND ----------

# Customer accounts can also *receive* money (TRANSFER / CASH_OUT to another C account).
# Counting both roles gives a fairer picture of how active an account is.
activity = (tx.select(F.col("nameOrig").alias("account"), "step", "week", "day", F.lit("send").alias("role"))
              .unionByName(tx.filter(~F.col("to_merchant"))
                             .select(F.col("nameDest").alias("account"), "step", "week", "day", F.lit("receive").alias("role"))))

per_account = activity.groupBy("account").agg(F.count("*").alias("events"))
display(per_account.agg(F.count("*").alias("accounts"),
                        F.round(F.avg("events"), 2).alias("avg_events"),
                        F.expr("percentile_approx(events, 0.5)").alias("median_events"),
                        F.round(100 * F.avg((F.col("events") > 1).cast("int")), 2).alias("pct_active_more_than_once")))

# COMMAND ----------

# MAGIC %md ## 2. Transaction-type mix (count vs value)

# COMMAND ----------

all_w = W.partitionBy()
type_mix = save(
    tx.filter("is_valid_amount").groupBy("type")
      .agg(F.count("*").alias("n_txns"),
           F.sum("amount").alias("value"),
           F.expr("percentile_approx(amount, 0.5)").alias("median_amount"),
           F.sum("isFraud").alias("fraud_n"))
      .withColumn("pct_txns", F.round(100 * F.col("n_txns") / F.sum("n_txns").over(all_w), 1))
      .withColumn("pct_value", F.round(100 * F.col("value") / F.sum("value").over(all_w), 1))
      .withColumn("value_bn", F.round(F.col("value") / 1e9, 1))
      .orderBy(F.desc("n_txns")),
    "gold_type_mix")
display(type_mix)

# COMMAND ----------

# MAGIC %md ## 3. Weekly cohorts and retention
# MAGIC The data covers only 743 hours (31 days), so **monthly** cohorts would give a single cohort.
# MAGIC Cohorts are therefore **weekly** (week = 168 steps). An account's cohort is the week of its first
# MAGIC activity (send or receive); retention in week *k* = share of the cohort active again *k* weeks later.

# COMMAND ----------

first_week = activity.groupBy("account").agg(F.min("week").alias("cohort_week"))
acct_weeks = activity.select("account", "week").distinct().join(first_week, "account")

cohort_counts = (acct_weeks
    .withColumn("weeks_since", F.col("week") - F.col("cohort_week"))
    .groupBy("cohort_week", "weeks_since")
    .agg(F.countDistinct("account").alias("active_accounts")))

cohort_size = (cohort_counts.filter("weeks_since = 0")
    .select("cohort_week", F.col("active_accounts").alias("cohort_size")))

retention = save(
    cohort_counts.join(cohort_size, "cohort_week")
      .withColumn("retention_pct", F.round(100 * F.col("active_accounts") / F.col("cohort_size"), 2))
      .orderBy("cohort_week", "weeks_since"),
    "gold_cohort_retention")
display(retention)

# COMMAND ----------

# Repeat use among senders only (the stricter definition)
display(per_sender.agg(
    F.count("*").alias("senders"),
    F.sum((F.col("n_txns") > 1).cast("int")).alias("repeat_senders"),
    F.round(100 * F.avg((F.col("n_txns") > 1).cast("int")), 3).alias("repeat_sender_pct")))

# COMMAND ----------

# MAGIC %md ## 4. Peak hours
# MAGIC Average transactions per hour of day (averaged over the 31 days so busy days do not dominate).

# COMMAND ----------

n_days = tx.select(F.countDistinct("day")).first()[0]
hourly = save(
    tx.groupBy("hour_of_day")
      .agg(F.count("*").alias("n_txns"),
           F.sum("isFraud").alias("fraud_n"),
           F.sum(F.when(F.col("isFraud") == 1, F.col("amount"))).alias("fraud_value"))
      .withColumn("avg_txns_per_day", F.round(F.col("n_txns") / F.lit(n_days), 0))
      .withColumn("fraud_rate_pct", F.round(100 * F.col("fraud_n") / F.col("n_txns"), 2))
      .orderBy("hour_of_day"),
    "gold_hourly")
display(hourly)

# COMMAND ----------

daily = save(tx.groupBy("day").agg(F.count("*").alias("n_txns"), F.sum("isFraud").alias("fraud_n")).orderBy("day"),
             "gold_daily")
display(daily)

# COMMAND ----------

# MAGIC %md ## 5. Fraud patterns

# COMMAND ----------

# 5a. Fraud by amount band (TRANSFER and CASH_OUT only: the only types with fraud)
risky = tx.filter(F.col("type").isin("TRANSFER", "CASH_OUT"))
band = (F.when(F.col("amount") < 10_000, "a) <10k")
         .when(F.col("amount") < 100_000, "b) 10k–100k")
         .when(F.col("amount") < 1_000_000, "c) 100k–1M")
         .when(F.col("amount") < 10_000_000, "d) 1M–10M")
         .otherwise("e) 10M (cap)"))
fraud_by_band = save(
    risky.withColumn("amount_band", band).groupBy("amount_band")
         .agg(F.count("*").alias("n_txns"), F.sum("isFraud").alias("fraud_n"))
         .withColumn("fraud_rate_pct", F.round(100 * F.col("fraud_n") / F.col("n_txns"), 2))
         .orderBy("amount_band"),
    "gold_fraud_by_amount")
display(fraud_by_band)

# COMMAND ----------

# 5b. Rule comparison: the built-in flag vs a simple "account drained" rule.
#     drain rule = TRANSFER or CASH_OUT whose amount equals the sender's full opening balance (> 0).
drain = (F.col("type").isin("TRANSFER", "CASH_OUT")
         & (F.col("oldbalanceOrg") > 0)
         & (F.abs(F.col("amount") - F.col("oldbalanceOrg")) < 0.01))

def rule_stats(name, pred):
    return (tx.withColumn("hit", pred.cast("int"))
              .agg(F.lit(name).alias("rule"),
                   F.sum("hit").alias("alerts"),
                   F.sum(F.col("hit") * F.col("isFraud")).alias("true_positives"),
                   F.sum("isFraud").alias("fraud_total")))

rules = save(
    rule_stats("Built-in isFlaggedFraud", F.col("isFlaggedFraud") == 1)
      .unionByName(rule_stats("Drain rule: amount = full balance", drain))
      .unionByName(rule_stats("Drain rule OR night (02–05h) transfer > 200k",
                              drain | (F.col("type").isin("TRANSFER", "CASH_OUT")
                                       & F.col("hour_of_day").between(2, 5)
                                       & (F.col("amount") > 200_000))))
      .withColumn("recall_pct", F.round(100 * F.col("true_positives") / F.col("fraud_total"), 1))
      .withColumn("precision_pct", F.round(100 * F.col("true_positives") / F.col("alerts"), 1)),
    "gold_fraud_rules")
display(rules)

# COMMAND ----------

# 5c. What a fraud case looks like: a TRANSFER that empties the account, then a CASH_OUT of the same amount.
pairs = (tx.filter("isFraud = 1 AND type = 'TRANSFER'").select("step", "amount").alias("t")
           .join(tx.filter("isFraud = 1 AND type = 'CASH_OUT'").select("step", "amount").alias("c"),
                 (F.col("t.step") == F.col("c.step")) & (F.col("t.amount") == F.col("c.amount"))))
display(pairs.agg(F.count("*").alias("transfer_cashout_pairs_same_step_same_amount")))

# COMMAND ----------

# MAGIC %md ## Headline numbers (used in the dashboard text and README)

# COMMAND ----------

kpis = save(spark.sql("""
  SELECT COUNT(*)                                          AS total_txns,
         ROUND(SUM(amount) / 1e9, 1)                       AS total_value_bn,
         COUNT(DISTINCT nameOrig)                          AS senders,
         SUM(isFraud)                                      AS fraud_txns,
         ROUND(100 * AVG(isFraud), 3)                      AS fraud_rate_pct,
         ROUND(SUM(CASE WHEN isFraud = 1 THEN amount END) / 1e9, 1) AS fraud_value_bn
  FROM silver_transactions"""), "gold_kpis")
display(kpis)
