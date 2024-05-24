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
    duration numeric
);


create table if not exists auth_events(
    ts bigint,
    sessionId int,
    level varchar(4),
    itemInSession int,
    city varchar(50),
    zip varchar(9),
    state char(2),
    userAgent varchar(200),
    lon numeric (6,3),
    lat numeric (6,3),
    userId int,
    lastname varchar(50),
    firstname varchar(50),
    gender char,
    registration bigint,
    success bool 
);


create table if not exists status_change_events(
    ts bigint,
    sessionId int,
    level varchar(4),
    itemInSession int,
    city varchar(50),
    zip varchar(9),
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

create table if not exists page_view_events(
    ts bigint,
    sessionId int,
    level varchar(4),
    itemInSession int,
    city varchar(50),
    zip varchar(9),
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

















