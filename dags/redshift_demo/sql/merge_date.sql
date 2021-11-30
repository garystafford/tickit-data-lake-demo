BEGIN TRANSACTION;

DELETE
FROM tickit_demo.date
    USING tickit_demo.date_staging
WHERE date.dateid = date_staging.dateid;

INSERT INTO tickit_demo.date
SELECT *
FROM tickit_demo.date_staging;

END TRANSACTION;