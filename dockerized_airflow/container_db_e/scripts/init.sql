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



COPY listen_events(
    trackId, 
    artist, 
    song, 
    duration, 
    ts, 
    sessionId, 
    auth, 
    level, 
    itemInSession, 
    city, 
    zip, 
    state, 
    userAgent, 
    lon, 
    lat, 
    userId, 
    lastName, 
    firstName, 
    gender, 
    registration
)
FROM '/docker-entrypoint-initdb.d/listen_events.csv'
DELIMITER ','
CSV HEADER
--para indicar que la primera lı́nea es el encabezado
ENCODING 'LATIN1';










