CREATE DATABASE tickit;
USE tickit;

CREATE TABLE IF NOT EXISTS tickit.listing
(
	listid int not null,
	sellerid int not null,
	eventid int not null,
	dateid smallint not null,
	numtickets smallint not null,
	priceperticket decimal(8,2) not null,
	totalprice decimal(8,2) not null,
	listtime timestamp not null
)
COMMENT 'TICKIT event listings for sale';

CREATE TABLE IF NOT EXISTS tickit.date(
	dateid smallint not null,
	caldate date not null,
	day character(3) not null,
	week smallint not null,
	month character(5) not null,
	qtr character(5) not null,
	year smallint not null,
	holiday boolean not null default 0
)
COMMENT 'TICKIT transaction date details';

CREATE TABLE IF NOT EXISTS tickit.sales
(
	salesid int not null,
	listid int not null,
	sellerid int not null,
	buyerid int not null,
	eventid int not null,
	dateid smallint not null,
	qtysold smallint not null,
	pricepaid decimal(8,2) not null,
	commission decimal(8,2) not null,
	saletime varchar(20) not null
)
COMMENT 'TICKIT sales transactions';

