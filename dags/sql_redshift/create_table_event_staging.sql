CREATE TABLE IF NOT EXISTS tickit_demo.event_staging
(
    eventid integer NOT NULL DISTKEY,
    venueid smallint NOT NULL,
    catid smallint NOT NULL,
    dateid smallint NOT NULL SORTKEY,
    eventname varchar(200),
    starttime timestamp
);
