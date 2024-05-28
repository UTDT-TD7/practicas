create table if not exists listen_events(
    sessionId int,
    itemInSession int,
    userId int,
    ts bigint,
    auth varchar(9),
    level varchar(4),
    trackId varchar(18),
    song varchar(500),
    artist varchar(500),
    zip varchar(9),
    city varchar(50),
    state char(2),
    userAgent varchar(200),
    lon numeric (6,3),
    lat numeric (6,3),
    lastname varchar(50),
    firstname varchar(50),
    gender char,
    registration bigint,
    duration numeric,
    fecha timestamp
);





create table if not exists etl_dump(
    sessionId int,
    itemInSession int,
    userId int,
    ts bigint,
    trackId varchar(18),
    zip varchar(9),
    len varchar (50),
    ban varchar (50),
    muestreo varchar (50)   
);

<<<<<<< HEAD
create table if not exists etl_dump_2(
    sessionId int,
    itemInSession int,
    userId int,
    ts bigint,
    trackId varchar(18),
    zip varchar(9),
    len varchar (50),
    ban varchar (50),
    muestreo varchar (50)   
);









=======
>>>>>>> 34bb84efab17e4220460eb9d7223b11d49e25048
















