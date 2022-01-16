BEGIN TRANSACTION;

CREATE TABLE tickit_demo.users
(
    userid integer NOT NULL DISTKEY SORTKEY,
    username char(8),
    firstname varchar(30),
    lastname varchar(30),
    city varchar(30),
    state char(2),
    email varchar(100),
    phone char(14),
    likesports boolean,
    liketheatre boolean,
    likeconcerts boolean,
    likejazz boolean,
    likeclassical boolean,
    likeopera boolean,
    likerock boolean,
    likevegas boolean,
    likebroadway boolean,
    likemusicals boolean
);

CREATE TABLE tickit_demo.venue
(
    venueid smallint NOT NULL DISTKEY SORTKEY,
    venuename varchar(100),
    venuecity varchar(30),
    venuestate char(2),
    venueseats integer
);

CREATE TABLE tickit_demo.category
(
    catid smallint NOT NULL DISTKEY SORTKEY,
    catgroup varchar(10),
    catname varchar(10),
    catdesc varchar(50)
);

CREATE TABLE tickit_demo.date
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

CREATE TABLE tickit_demo.event
(
    eventid integer NOT NULL DISTKEY,
    venueid smallint NOT NULL,
    catid smallint NOT NULL,
    dateid smallint NOT NULL SORTKEY,
    eventname varchar(200),
    starttime timestamp
);

CREATE TABLE tickit_demo.listing
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

CREATE TABLE tickit_demo.sales
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

END TRANSACTION;
