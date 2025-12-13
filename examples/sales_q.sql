SELECT region, SUM(amount)
FROM sales
WHERE amount > 100
GROUP BY region;
