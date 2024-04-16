CREATE TABLE IF NOT EXISTS ejemplo (
    id SERIAL PRIMARY KEY,
    texto TEXT NOT NULL
);

INSERT INTO ejemplo (texto) VALUES ('texto 1');
INSERT INTO ejemplo (texto) VALUES ('texto 2');
