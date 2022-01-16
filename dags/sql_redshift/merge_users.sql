BEGIN TRANSACTION;

DELETE
FROM tickit_demo.users
    USING tickit_demo.users_staging
WHERE users.userid = users_staging.userid;

INSERT INTO tickit_demo.users
SELECT
    userid,
    username,
    firstname,
    lastname,
    city,
    state,
    email,
    phone,
    likesports,
    liketheatre,
    likeconcerts,
    likejazz,
    likeclassical,
    likeopera,
    likerock,
    likevegas,
    likebroadway,
    likemusicals
FROM tickit_demo.users_staging;

END TRANSACTION;
