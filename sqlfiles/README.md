## Fix and flip loans per lender per year
Query present in: `fixAndFlips.sql` file

Self join table to get transactions per property. Result of this: transaction per property and the next (later) transactions
Get only the next transaction of a transaction per property using window function (RANK)
Find counts of fix and flip loans per lender per year