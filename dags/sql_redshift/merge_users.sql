BEGIN TRANSACTION;

DELETE
FROM tickit_demo.users
    USING tickit_demo.users_staging
WHERE users.userid = users_staging.userid;

INSERT INTO tickit_demo.users
SELECT *
FROM tickit_demo.users_staging;

END TRANSACTION;