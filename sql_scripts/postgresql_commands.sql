-- find events with no corresponding venue
CREATE DATABASE tickit;

CREATE SCHEMA saas;

CREATE TABLE saas.venue(
	venueid smallint not null,
	venuename varchar(100),
	venuecity varchar(30),
	venuestate char(2),
	venueseats integer);

CREATE TABLE saas.category(
	catid smallint not null,
	catgroup varchar(10),
	catname varchar(10),
	catdesc varchar(50));

CREATE TABLE saas.event(
	eventid integer not null,
	venueid smallint not null,
	catid smallint not null,
	dateid smallint not null,
	eventname varchar(200),
	starttime timestamp);

VACUUM ANALYZE;
VACUUM FULL;

SELECT *
FROM event
WHERE venueid NOT IN (
    SELECT venueid
    FROM venue);

-- Get row counts for all tables
SELECT schemaname, relname, n_live_tup
FROM pg_stat_user_tables
WHERE schemaname = 'public'
ORDER BY n_live_tup DESC;