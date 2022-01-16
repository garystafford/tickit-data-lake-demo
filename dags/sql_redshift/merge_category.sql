BEGIN TRANSACTION;

DELETE
FROM tickit_demo.category
    USING tickit_demo.category_staging
WHERE category.catid = category_staging.catid;

INSERT INTO tickit_demo.category
SELECT
    catid,
    catgroup,
    catname,
    catdesc
FROM tickit_demo.category_staging;

END TRANSACTION;
