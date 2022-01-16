CREATE TABLE IF NOT EXISTS tickit_demo.date_staging
(
    dateid smallint NOT NULL DISTKEY SORTKEY,
    caldate date NOT NULL,
    day character(3) NOT NULL,
    week smallint NOT NULL,
    month character(5) NOT NULL,
    qtr character(5) NOT NULL,
    year smallint NOT NULL,
    holiday boolean DEFAULT ('N')
);
