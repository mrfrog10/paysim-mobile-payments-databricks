import json, sys
S = "workspace.paysim."
datasets = [
 ("kpis", "KPIs", f"SELECT * FROM {S}gold_kpis"),
 ("types", "Type mix", f"""SELECT type, 'Share of transactions (%)' AS measure, pct_txns AS pct FROM {S}gold_type_mix
UNION ALL SELECT type, 'Share of value (%)', pct_value FROM {S}gold_type_mix"""),
 ("hourly", "Hourly", f"SELECT hour_of_day, avg_txns_per_day, fraud_rate_pct, fraud_n FROM {S}gold_hourly"),
 ("ret", "Retention", f"""SELECT concat('Week ', cohort_week, ' cohort') AS cohort, weeks_since, retention_pct
FROM {S}gold_cohort_retention WHERE weeks_since > 0 AND cohort_week <= 3"""),
 ("rules", "Fraud rules", f"""WITH r AS (SELECT CASE WHEN rule LIKE 'Built-in%' THEN '1 Built-in flag'
                          WHEN rule LIKE '%OR night%' THEN '3 Drain + night rule'
                          ELSE '2 Drain rule' END AS rule, recall_pct, precision_pct FROM {S}gold_fraud_rules)
SELECT rule, 'Recall (%)' AS metric, recall_pct AS pct FROM r
UNION ALL SELECT rule, 'Precision (%)', precision_pct FROM r"""),
]
ds = [{"name": n, "displayName": d, "queryLines": [l + "\n" for l in q.split("\n")]} for n, d, q in datasets]

def f(name): return {"name": name, "expression": f"`{name}`"}
def chart(name, dsname, fields, wtype, enc, title, pos, version=3):
    return {"widget": {"name": name,
        "queries": [{"name": "main_query", "query": {"datasetName": dsname, "fields": [f(x) for x in fields], "disaggregated": True}}],
        "spec": {"version": version, "widgetType": wtype, "encodings": enc,
                 **({"mark": {"layout": "group"}} if wtype == "bar" and "color" in enc else {}),
                 "frame": {"showTitle": True, "title": title}}},
        "position": dict(zip(["x", "y", "width", "height"], pos))}
def ax(field, kind, label): return {"fieldName": field, "scale": {"type": kind}, "displayName": label}
def text(name, lines, pos):
    return {"widget": {"name": name, "multilineTextboxSpec": {"lines": lines}},
            "position": dict(zip(["x", "y", "width", "height"], pos))}
def counter(name, field, title, pos):
    return chart(name, "kpis", [field], "counter", {"value": {"fieldName": field, "displayName": title}}, title, pos, 2)

layout = [
 text("title", ["## Mobile payments: usage and fraud\n",
                "PaySim synthetic mobile-money data: 6.36M transactions over 31 simulated days. SQL + PySpark on Delta tables.\n"], [0, 0, 6, 2]),
 counter("k1", "total_txns", "Transactions", [0, 2, 2, 2]),
 counter("k2", "total_value_bn", "Value moved (bn)", [2, 2, 2, 2]),
 counter("k3", "fraud_rate_pct", "Fraud rate (% of txns)", [4, 2, 2, 2]),
 chart("c1", "types", ["type", "measure", "pct"], "bar",
       {"x": ax("type", "categorical", "Transaction type"), "y": ax("pct", "quantitative", "%"),
        "color": ax("measure", "categorical", "Measure")}, "1 · What people use it for: count vs value", [0, 4, 3, 6]),
 chart("c2", "hourly", ["hour_of_day", "avg_txns_per_day"], "bar",
       {"x": ax("hour_of_day", "categorical", "Hour of day"), "y": ax("avg_txns_per_day", "quantitative", "Avg transactions per day")},
       "2 · Peak hours: average transactions per hour", [3, 4, 3, 6]),
 chart("c3", "hourly", ["hour_of_day", "fraud_rate_pct"], "line",
       {"x": ax("hour_of_day", "quantitative", "Hour of day"), "y": ax("fraud_rate_pct", "quantitative", "Fraud rate (%)")},
       "3 · Fraud rate by hour: ~340 frauds every hour, so night rate jumps", [0, 10, 3, 6]),
 chart("c4", "ret", ["cohort", "weeks_since", "retention_pct"], "line",
       {"x": ax("weeks_since", "quantitative", "Weeks since first activity"), "y": ax("retention_pct", "quantitative", "Accounts active again (%)"),
        "color": ax("cohort", "categorical", "Cohort")}, "4 · Weekly cohort retention (send or receive)", [3, 10, 3, 6]),
 chart("c5", "rules", ["rule", "metric", "pct"], "bar",
       {"x": ax("rule", "categorical", "Rule"), "y": ax("pct", "quantitative", "%"), "color": ax("metric", "categorical", "Metric")},
       "5 · Fraud rules: built-in flag vs account-drain rule", [0, 16, 3, 9]),
 text("insights", [line + "\n" for line in open(sys.argv[2], encoding="utf-8").read().split("\n")], [3, 16, 3, 9]),
]
dash = {"datasets": ds, "pages": [{"name": "overview", "displayName": "Overview", "layout": layout}]}
json.dump(dash, open(sys.argv[1], "w", encoding="utf-8"), indent=1, ensure_ascii=False)
print("ok")
