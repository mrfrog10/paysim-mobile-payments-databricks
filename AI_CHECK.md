# AI check: where the assistant was wrong and how it was caught

An AI coding assistant wrote a first draft of most of the SQL and PySpark code here. None of its output was accepted without a check. I used three kinds of check:

1. **Run it and read the error.** Syntax and API mistakes show up straight away.
2. **Test assumptions against the data.** Each claim about the data (time span, ID reuse, flag rules) gets a query that could prove it false.
3. **Recompute with a second engine.** [`verification/crosscheck.py`](verification/crosscheck.py) recomputes the headline numbers in DuckDB, written as plain SQL and separately from the PySpark notebooks. Both engines must give the same result. Output: [`verification/crosscheck_output.json`](verification/crosscheck_output.json).

## What it got wrong

| # | What the AI proposed or wrote | What was wrong | How it was caught | Fix |
|---|---|---|---|---|
| 1 | Plan: "analyse **monthly** cohorts" | `step` runs 1–743 hours, so the data covers **31 days**. Monthly cohorts would give one cohort and no retention curve. | `SELECT MIN(step), MAX(step)` in profiling | Weekly cohorts (168 steps) |
| 2 | Plan: retention as "repeat use" of senders (`nameOrig`) | **99.85%** of senders appear exactly once, so sender retention is about 0% and tells you nothing | Distribution of transactions per `nameOrig` (6,344,009 senders with 1 transaction, 9,283 with 2, 15 with 3) | Account activity = sending **or** receiving; result stated alongside the strict sender figure |
| 3 | Hour of day as `step % 24` in one query, day as `(step-1) DIV 24` in another | The two time conventions don't agree. Step 24 was "hour 0" but "day 1", so every hour label was off by one, and the night fraud peak was reported as 03:00–05:59 instead of **02:00–04:59** | Hand check: step 1 and step 24 should land in the same day | One convention everywhere: `hour_index = step - 1` |
| 4 | DuckDB queries aliased columns as `hour` and `day` | Reserved keywords in DuckDB: parser error | Running the query | Renamed the aliases |
| 5 | First "drain" fraud rule: `amount = oldbalanceOrg` | Also matched 16 rows where amount **and** balance were both 0, which inflated the hit count with trivial matches | Looked at rows hit by the rule but not by the stricter version | Added `oldbalanceOrg > 0` and a 1-cent tolerance. Result: 8,018 alerts, all fraud |
| 6 | Notebook comment repeated the dataset description: `isFlaggedFraud` fires on transfers above 200,000 | Not what the data shows: **409,110** transfers exceed 200k but only **16** are flagged (smallest flagged amount: 353,874) | Query in notebook 01, section 3 | Treat the flag as a black-box baseline and measure its recall (0.2%) |

## Checks that passed (the AI was right)

- Row count 6,362,620. No nulls, no exact duplicates, no unknown types.
- Fraud happens only in TRANSFER and CASH_OUT.
- `M…` receivers appear only on PAYMENT.
- The 16 non-positive amounts are all zero-amount CASH_OUT fraud rows. They are excluded from usage metrics and kept for fraud analysis.

## Spark vs DuckDB comparison

| Metric | DuckDB (local) | Databricks (Spark) |
|---|---|---|
| Rows | 6,362,620 | 6,362,620 ✅ |
| Repeat-sender share | 0.146% | 0.146% ✅ |
| Week-1 cohort → week-2 retention | 8.22% | 8.22% ✅ |
| Fraud rate 03:00–03:59 | 22.08% | 22.08% ✅ |
| Built-in flag recall / precision | 0.2% / 100% | 0.2% / 100% ✅ |
| Drain rule recall / precision | 97.6% / 100% | 97.6% / 100% ✅ |
| Type mix (PAYMENT: % of txns / % of value) | 33.8% / 2.5% | 33.8% / 2.5% ✅ |

All metrics match exactly. The DuckDB code was written as plain SQL, separately from the PySpark DataFrame code, so a logic error would have to happen in both to go unnoticed.

## Lesson

The assistant was fastest at boilerplate: schemas, window functions and chart specs. It was weakest on **assumptions about the data**: time span, what counts as a "user", and what a documented flag really does. Those mistakes don't cause an error. They give a believable wrong answer. So every assumption gets its own query.
