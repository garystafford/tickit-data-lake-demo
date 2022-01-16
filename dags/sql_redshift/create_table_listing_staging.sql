CREATE TABLE IF NOT EXISTS tickit_demo.listing_staging
(
    listid integer NOT NULL DISTKEY,
    sellerid integer NOT NULL,
    eventid integer NOT NULL,
    dateid smallint NOT NULL SORTKEY,
    numtickets smallint NOT NULL,
    priceperticket decimal(8, 2),
    totalprice decimal(8, 2),
    listtime timestamp
);
