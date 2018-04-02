SELECT alender, EXTRACT(YEAR FROM adate) AS year_originated, COUNT(1) AS fix_and_flips FROM
(SELECT aid, alender, adate, bid, blender, bdate, rank() OVER (PARTITION BY aid ORDER BY bdate) as rank
FROM
(SELECT a.id as aid, a.transaction_date as adate, a.property_id as apid, a.lender as alender,
b.id as bid, b.transaction_date as bdate, b.property_id as bpid, b.lender as blender
FROM transactions a INNER JOIN transactions b
ON a.property_id = b.property_id
WHERE a.lender IS NOT NULL
AND a.id != b.id
AND a.transaction_date <= b.transaction_date
) x
) y
WHERE rank = 1
AND bdate::DATE - adate::DATE <= 365
GROUP BY 1, 2
ORDER BY 2;