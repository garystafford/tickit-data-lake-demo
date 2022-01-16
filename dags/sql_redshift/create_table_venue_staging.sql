CREATE TABLE IF NOT EXISTS tickit_demo.venue_staging
(
    venueid smallint NOT NULL DISTKEY SORTKEY,
    venuename varchar(100),
    venuecity varchar(30),
    venuestate char(2),
    venueseats integer
);
