-- save as view - view_tickit_sales_by_month
SELECT d.year,
	d.month,
	round(sum(cast(s.pricepaid AS DECIMAL(8,2)) * s.qtysold), 2) AS sum_sales,
	round(sum(cast(s.commission AS DECIMAL(8,2))), 2) AS sum_commission,
	count(*) AS order_volume
FROM refined_tickit_public_sales AS s
	JOIN refined_tickit_public_date AS d USING(dateid)
GROUP BY d.year,
	d.month
ORDER BY d.year,
	d.month;


-- ctas gold/agg #1
CREATE TABLE IF NOT EXISTS agg_tickit_sales_by_category
WITH (
	format = 'Parquet',
	write_compression = 'SNAPPY',
	external_location = 's3://open-data-lake-demo-us-east-1/tickit/gold/tickit_sales_by_category/',
	partitioned_by = ARRAY [ 'catgroup',
	'catname' ],
	bucketed_by = ARRAY [ 'bucket_catname' ],
	bucket_count = 1
)
AS WITH cat AS (
	SELECT DISTINCT e.eventid,
		c.catgroup,
		c.catname
	FROM refined_tickit_public_event AS e
		LEFT JOIN refined_tickit_public_category AS c ON c.catid = e.catid
)
SELECT cast(d.caldate AS DATE) AS caldate,
	s.pricepaid,
	s.qtysold,
	round(cast(s.pricepaid AS DECIMAL(8,2)) * s.qtysold, 2) AS sale_amount,
	cast(s.commission AS DECIMAL(8,2)) AS commission,
	round((cast(s.commission AS DECIMAL(8,2)) / (cast(s.pricepaid AS DECIMAL(8,2)) * s.qtysold)) * 100, 2) AS commission_prcnt,
	e.eventname,
	concat(u1.firstname, ' ', u1.lastname) AS seller,
	concat(u2.firstname, ' ', u2.lastname) AS buyer,
	c.catname AS bucket_catname,
	c.catgroup,
	c.catname
FROM refined_tickit_public_sales AS s
	LEFT JOIN refined_tickit_public_listing AS l ON l.listid = s.listid
	LEFT JOIN refined_tickit_public_users AS u1 ON u1.userid = s.sellerid
	LEFT JOIN refined_tickit_public_users AS u2 ON u2.userid = s.buyerid
	LEFT JOIN refined_tickit_public_event AS e ON e.eventid = s.eventid
	LEFT JOIN refined_tickit_public_date AS d ON d.dateid = s.dateid
	LEFT JOIN cat AS c ON c.eventid = s.eventid;


-- ctas gold/agg #2
CREATE TABLE IF NOT EXISTS agg_tickit_sales_by_date
WITH (
	format = 'Parquet',
	write_compression = 'SNAPPY',
	external_location = 's3://open-data-lake-demo-us-east-1/tickit/gold/tickit_sales_by_date/',
	partitioned_by = ARRAY [ 'year', 'month'],
	bucketed_by = ARRAY [ 'bucket_month' ],
	bucket_count = 1
)
AS WITH cat AS (
	SELECT DISTINCT e.eventid,
		c.catgroup,
		c.catname
	FROM refined_tickit_public_event AS e
		LEFT JOIN refined_tickit_public_category AS c ON c.catid = e.catid
)
SELECT cast(d.caldate AS DATE) AS caldate,
	s.pricepaid,
	s.qtysold,
	round(cast(s.pricepaid AS DECIMAL(8,2)) * s.qtysold, 2) AS sale_amount,
	cast(s.commission AS DECIMAL(8,2)) AS commission,
	round((cast(s.commission AS DECIMAL(8,2)) / (cast(s.pricepaid AS DECIMAL(8,2)) * s.qtysold)) * 100, 2) AS commission_prcnt,
	e.eventname,
	concat(u1.firstname, ' ', u1.lastname) AS seller,
	concat(u2.firstname, ' ', u2.lastname) AS buyer,
	c.catgroup,
	c.catname,
	d.month AS bucket_month,
	d.year,
	d.month
FROM refined_tickit_public_sales AS s
	LEFT JOIN refined_tickit_public_listing AS l ON l.listid = s.listid
	LEFT JOIN refined_tickit_public_users AS u1 ON u1.userid = s.sellerid
	LEFT JOIN refined_tickit_public_users AS u2 ON u2.userid = s.buyerid
	LEFT JOIN refined_tickit_public_event AS e ON e.eventid = s.eventid
	LEFT JOIN refined_tickit_public_date AS d ON d.dateid = s.dateid
	LEFT JOIN cat AS c ON c.eventid = s.eventid;


SELECT * FROM agg_tickit_sales_by_date
WHERE catgroup = 'Concerts'
LIMIT 10;


-- query gold/agg data
-- save as view - view_agg_sales_by_month
SELECT year(caldate) AS sales_year,
	month(caldate) AS sales_month,
	round(sum(sale_amount), 2) AS sum_sales,
	round(sum(commission), 2) AS sum_commission,
	count(*) AS order_volume
FROM agg_tickit_sales_by_category
GROUP BY year(caldate),
	month(caldate)
ORDER BY year(caldate),
	month(caldate);


SELECT year(caldate) AS sales_year,
	month(caldate) AS sales_month,
	round(sum(sale_amount), 2) AS sum_sales,
	round(sum(commission), 2) AS sum_commission,
	count(*) AS order_volume
FROM agg_tickit_sales_by_date
GROUP BY year(caldate),
	month(caldate)
ORDER BY year(caldate),
	month(caldate);


-- efficiency of partitions and columns with Parquet
SELECT *
FROM "tickit_demo"."agg_tickit_sales_by_category"
LIMIT 10000;

SELECT *
FROM "tickit_demo"."agg_tickit_sales_by_category"
WHERE catgroup = 'Shows' AND catname = 'Opera'
LIMIT 10000;

SELECT caldate, sale_amount, commission
FROM "tickit_demo"."agg_tickit_sales_by_category"
WHERE catgroup = 'Shows' AND catname = 'Opera'
LIMIT 10000;