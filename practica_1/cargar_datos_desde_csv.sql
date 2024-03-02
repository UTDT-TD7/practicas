COPY auth_events (
    ts, 
    sessionId, 
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
    registration, 
    success
)
FROM '/data/auth_events.csv'
DELIMITER ','
CSV HEADER
--para indicar que la primera lı́nea es el encabezado
ENCODING 'LATIN1';


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
FROM '/data/listen_events.csv'
DELIMITER ','
CSV HEADER
--para indicar que la primera lı́nea es el encabezado
ENCODING 'LATIN1';


COPY page_view_events (
    ts, 
    sessionId, 
    page, 
    auth, 
    method, 
    status, 
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
    registration, 
    trackId, 
    artist, 
    song, 
    duration
)
FROM '/data/page_view_events.csv'
DELIMITER ','
CSV HEADER
--para indicar que la primera lı́nea es el encabezado
ENCODING 'LATIN1';


COPY status_change_events (
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
FROM '/data/status_change_events.csv'
DELIMITER ','
CSV HEADER
--para indicar que la primera lı́nea es el encabezado
ENCODING 'LATIN1';
