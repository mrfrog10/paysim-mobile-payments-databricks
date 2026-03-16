## 3 insights for the product team

**1. Fraud is a night-time problem.** About 340 fraudulent transactions happen every hour, day or night, but genuine use falls by 99% overnight. Between 02:00 and 04:59 roughly 1 in 5 transactions is fraud (16–22%), against 0.07% at midday. *Action:* add step-up checks (e.g. in-app confirmation or a short hold) for TRANSFER and CASH_OUT between 01:00 and 06:00. Few genuine users would notice.

**2. The current fraud flag misses 99.8% of fraud.** `isFlaggedFraud` fired 16 times out of 8,213 frauds. One simple rule, a TRANSFER or CASH_OUT that empties the sender's balance, catches 97.6% with no false alarms in this data. *Action:* make "full-balance withdrawal" a high-priority alert. Check it against real data first, because a simulator can make one rule look better than it is.

**3. Payments are frequent but small, and few users come back.** PAYMENT is 34% of transactions but only 2.5% of value. Only 8% of week-1 accounts are active again in week 2, and 99.85% of senders send once. *Action:* the habit-forming product is merchant payments. Test re-engagement after the first payment (cashback on a second payment within 7 days), and track week-2 retention as the main metric.
