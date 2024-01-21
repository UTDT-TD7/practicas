create table users(
userid int,
firstname varchar(50),
lastname varchar(50),
gender char,
registration bigint);

create table location(
zip int,
state char(2),
city varchar(50),
lat numeric (6,3),
lon numeric (6,3)
);


COPY location
FROM '/location.csv'
DELIMITER ','
CSV HEADER
--para indicar que la primera lı́nea es el encabezado
ENCODING 'LATIN1';

COPY users
FROM '/users.csv'
DELIMITER ','
CSV HEADER
--para indicar que la primera lı́nea es el encabezado
ENCODING 'LATIN1';
