CREATE TABLE IF NOT EXISTS tickit_demo.category_staging
(
    catid smallint NOT NULL DISTKEY SORTKEY,
    catgroup varchar(10),
    catname varchar(10),
    catdesc varchar(50)
);
