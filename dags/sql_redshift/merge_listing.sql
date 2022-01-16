BEGIN TRANSACTION;

DELETE
FROM tickit_demo.listing
    USING tickit_demo.listing_staging
WHERE listing.listid = listing_staging.listid
  AND listing_staging.listtime BETWEEN '{{ params.begin_date }}' AND '{{ params.end_date }}';

INSERT INTO tickit_demo.listing
SELECT listid,
       sellerid,
       eventid,
       dateid,
       numtickets,
       priceperticket,
       totalprice,
       listtime
FROM tickit_demo.listing_staging
WHERE listing_staging.listtime BETWEEN '{{ params.begin_date }}' AND '{{ params.end_date }}';

END TRANSACTION;