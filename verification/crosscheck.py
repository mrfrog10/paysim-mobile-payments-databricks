"""Independent re-computation of the key Databricks results with DuckDB on a laptop.

Written separately from the PySpark code (different engine, SQL instead of DataFrame API),
so a logic error in one is unlikely to repeat in the other. Compare the printed numbers
with the gold_* tables in Databricks.

Usage:  python crosscheck.py path/to/PS_20174392719_1491204439457_log.csv
"""
import json
import sys

import duckdb

con = duckdb.connect()
con.execute(f"CREATE TABLE t AS SELECT * FROM read_csv_auto('{sys.argv[1]}')")
con.execute("""CREATE TABLE s AS SELECT *, step - 1 AS hour_index, (step - 1) // 24 + 1 AS d,
               (step - 1) % 24 AS hod, (step - 1) // 168 + 1 AS wk FROM t""")

def one(sql):
    return con.execute(sql).fetchone()

def rows(sql):
    return con.execute(sql).fetchall()

out = {}
out["rows"] = one("SELECT COUNT(*) FROM t")[0]
out["non_positive_amounts"] = rows("SELECT type, isFraud, COUNT(*) FROM t WHERE amount <= 0 GROUP BY ALL")
out["exact_duplicates"] = one("SELECT COUNT(*) - (SELECT COUNT(*) FROM (SELECT DISTINCT * FROM t)) FROM t")[0]
out["merchant_not_payment"] = one("SELECT COUNT(*) FROM t WHERE nameDest LIKE 'M%' AND type <> 'PAYMENT'")[0]
out["orig_balance_mismatch"] = one("""SELECT COUNT(*) FROM t WHERE type IN ('TRANSFER','CASH_OUT')
                                      AND abs(oldbalanceOrg - amount - newbalanceOrig) > 0.01""")[0]
out["flag_claim_transfers_over_200k"] = one("""SELECT COUNT(*) FILTER (WHERE type='TRANSFER' AND amount>200000),
       COUNT(*) FILTER (WHERE type='TRANSFER' AND amount>200000 AND isFlaggedFraud=1) FROM t""")
out["flagged_amount_range"] = one("SELECT MIN(amount), MAX(amount) FROM t WHERE isFlaggedFraud = 1")
out["repeat_sender_pct"] = one("""SELECT ROUND(100.0 * AVG(CASE WHEN n > 1 THEN 1 ELSE 0 END), 3)
                                  FROM (SELECT nameOrig, COUNT(*) n FROM t GROUP BY 1)""")[0]
out["type_mix"] = rows("""SELECT type, COUNT(*), ROUND(100.0*COUNT(*)/SUM(COUNT(*)) OVER (),1),
                          ROUND(100.0*SUM(amount)/SUM(SUM(amount)) OVER (),1) FROM t WHERE amount > 0
                          GROUP BY 1 ORDER BY 2 DESC""")
out["retention_week1_cohort"] = rows("""
  WITH a AS (SELECT nameOrig acct, wk FROM s UNION ALL SELECT nameDest, wk FROM s WHERE nameDest LIKE 'C%'),
       f AS (SELECT acct, MIN(wk) c FROM a GROUP BY 1),
       j AS (SELECT f.c, a.wk - f.c k, COUNT(DISTINCT a.acct) u FROM a JOIN f USING (acct) GROUP BY 1, 2)
  SELECT k, u, ROUND(100.0 * u / MAX(CASE WHEN k = 0 THEN u END) OVER (), 2) FROM j WHERE c = 1 ORDER BY k""")
out["hourly"] = rows("""SELECT hod, COUNT(*), SUM(isFraud), ROUND(100.0*AVG(isFraud),2) FROM s GROUP BY 1 ORDER BY 1""")
drain = "type IN ('TRANSFER','CASH_OUT') AND oldbalanceOrg > 0 AND abs(amount - oldbalanceOrg) < 0.01"
night = "type IN ('TRANSFER','CASH_OUT') AND hod BETWEEN 2 AND 5 AND amount > 200000"
for name, pred in {"flag": "isFlaggedFraud = 1", "drain": drain, "drain_or_night": f"({drain}) OR ({night})"}.items():
    alerts, tp = one(f"SELECT COUNT(*), SUM(isFraud) FROM s WHERE {pred}")
    out[f"rule_{name}"] = {"alerts": alerts, "tp": int(tp or 0),
                           "recall": round(100 * (tp or 0) / 8213, 1),
                           "precision": round(100 * (tp or 0) / alerts, 1) if alerts else None}
out["transfer_cashout_pairs"] = one("""SELECT COUNT(*) FROM
   (SELECT step, amount FROM t WHERE isFraud=1 AND type='TRANSFER') a JOIN
   (SELECT step, amount FROM t WHERE isFraud=1 AND type='CASH_OUT') b USING (step, amount)""")[0]

print(json.dumps(out, indent=1, default=str))
