UNLOAD ('WITH cat AS (
        SELECT DISTINCT e.eventid,
            c.catgroup,
            c.catname
        FROM tickit_demo.event AS e
            LEFT JOIN tickit_demo.category AS c ON c.catid = e.catid
        )
    SELECT CAST(s.saletime AS VARCHAR(20)),
        s.pricepaid,
        s.qtysold,
        ROUND(CAST(s.pricepaid AS DECIMAL(8, 2)) * s.qtysold, 2) AS sale_amount,
        CAST(s.commission AS DECIMAL(8, 2)) AS commission,
        ROUND((CAST(s.commission AS DECIMAL(8, 2)) / (CAST(s.pricepaid AS DECIMAL(8, 2)) * s.qtysold)) * 100,
            2) AS commission_prcnt,
        e.eventname,
        u1.firstname || CHR(32) || u1.lastname AS seller,
        u2.firstname || CHR(32) || u2.lastname AS buyer,
        c.catgroup,
        c.catname
    FROM tickit_demo.sales AS s
        LEFT JOIN tickit_demo.listing AS l ON l.listid = s.listid
        LEFT JOIN tickit_demo.users AS u1 ON u1.userid = s.sellerid
        LEFT JOIN tickit_demo.users AS u2 ON u2.userid = s.buyerid
        LEFT JOIN tickit_demo.event AS e ON e.eventid = s.eventid
        LEFT JOIN tickit_demo.date AS d ON d.dateid = s.dateid
        LEFT JOIN cat AS c ON c.eventid = s.eventid
    ORDER BY caldate, sale_amount;')
TO '{{ params.s3_unload_path }}'
IAM_ROLE '{{ params.redshift_unload_iam_role }}'
PARQUET PARTITION BY (catgroup, catname) CLEANPATH;

