BEGIN TRANSACTION;

DELETE
FROM tickit_demo.sales
    USING tickit_demo.sales_staging
WHERE sales.salesid = sales_staging.salesid
  AND sales_staging.saletime BETWEEN '{{ params.begin_date }}' AND '{{ params.end_date }}';

INSERT INTO tickit_demo.sales
SELECT salesid,
       listid,
       sellerid,
       buyerid,
       eventid,
       dateid,
       qtysold,
       pricepaid,
       commission,
       saletime
FROM tickit_demo.sales_staging
WHERE sales_staging.saletime BETWEEN '{{ params.begin_date }}' AND '{{ params.end_date }}';

END TRANSACTION;