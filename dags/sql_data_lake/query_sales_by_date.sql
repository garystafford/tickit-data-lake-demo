SELECT
    year(caldate) AS sales_year,
    month(caldate) AS sales_month,
    round(sum(sale_amount), 2) AS sum_sales,
    round(sum(commission), 2) AS sum_commission,
    count(*) AS order_volume
FROM agg_tickit_sales_by_category
GROUP BY year(caldate), month(caldate)
ORDER BY year(caldate), month(caldate);
