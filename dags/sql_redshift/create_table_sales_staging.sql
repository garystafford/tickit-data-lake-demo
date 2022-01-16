CREATE TABLE IF NOT EXISTS tickit_demo.sales_staging
(
    salesid integer NOT NULL,
    listid integer NOT NULL DISTKEY,
    sellerid integer NOT NULL,
    buyerid integer NOT NULL,
    eventid integer NOT NULL,
    dateid smallint NOT NULL SORTKEY,
    qtysold smallint NOT NULL,
    pricepaid decimal(8, 2),
    commission decimal(8, 2),
    saletime timestamp
);
