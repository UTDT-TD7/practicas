create table listen_events(
sessionId int,
itemInSession int,
userId int,
ts bigint,
auth varchar(9),
level varchar(4),
trackId varchar(18),
song varchar(500),
artist varchar(500),
zip int,
city varchar(50),
state char(2),
userAgent varchar(200),
lon numeric (6,3),
lat numeric (6,3),
lastname varchar(50),
firstname varchar(50),
gender char,
registration bigint,
duration numeric);


create table auth_events(
ts bigint ,
sessionId int ,
level varchar(4) ,
itemInSession int ,
city varchar(50) ,
zip int ,
state char(2) ,
userAgent varchar(200) ,
lon numeric (6,3) ,
lat numeric (6,3) ,
userId int,
lastname varchar(50),
firstname varchar(50),
gender char ,
registration bigint,
success bool 
);


create table status_change_events(
ts bigint,
sessionId int,
level varchar(4),
itemInSession int,
city varchar(50),
zip int,
state char(2),
userAgent varchar(200),
lon numeric (6,3),
lat numeric (6,3),
userId int,
lastname varchar(50),
firstname varchar(50),
gender char,
registration bigint,
auth varchar(20)
);

create table page_view_events(
ts bigint,
sessionId int,
level varchar(4),
itemInSession int,
city varchar(50),
zip int,
state char(2),
userAgent varchar(200),
lon numeric (6,3),
lat numeric (6,3),
userId int,
lastname varchar(50),
firstname varchar(50),
gender char,
registration bigint,
page varchar(30),
auth varchar(20),
method varchar(3),
status int,
trackId varchar(18),
artist varchar(500),
song varchar(500),
duration numeric
);


COPY auth_events
FROM '/auth_events.csv'
DELIMITER ','
CSV HEADER
--para indicar que la primera lı́nea es el encabezado
ENCODING 'LATIN1';


COPY listen_events
FROM '/listen_events.csv'
DELIMITER ','
CSV HEADER
--para indicar que la primera lı́nea es el encabezado
ENCODING 'LATIN1';


COPY page_view_events
FROM '/page_views_events.csv'
DELIMITER ','
CSV HEADER
--para indicar que la primera lı́nea es el encabezado
ENCODING 'LATIN1';


COPY status_change_events
FROM '/status_change_events.csv'
DELIMITER ','
CSV HEADER
--para indicar que la primera lı́nea es el encabezado
ENCODING 'LATIN1';
