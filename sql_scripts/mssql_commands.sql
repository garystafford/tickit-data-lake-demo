CREATE DATABASE tickit;

USE tickit;

CREATE SCHEMA crm;

CREATE TABLE crm.users(
	userid integer not null,
	username char(8),
	firstname varchar(30),
	lastname varchar(30),
	city varchar(30),
	state char(2),
	email varchar(100),
	phone char(14),
	likesports BIT,
	liketheatre BIT,
	likeconcerts BIT,
	likejazz BIT,
	likeclassical BIT,
	likeopera BIT,
	likerock BIT,
	likevegas BIT,
	likebroadway BIT,
	likemusicals BIT);
