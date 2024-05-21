DROP TABLE IF EXISTS llamados_107_covid;
CREATE TABLE llamados_107_covid ( 
    FECHA varchar(20),
    COVID_LLAMADOS integer,
    CASOS_SOSPECHOSOS integer,
    CASOS_DESCARTADOS_COVID integer,
    CASOS_TRASLADADOS integer,
    CASOS_DERIVADOS integer
);
DROP TABLE IF EXISTS viajes_sube;
CREATE TABLE IF NOT EXISTS viajes_sube ( 
    TIPO_TRANSPORTE varchar(10),
    DIA varchar(20),
    PARCIAL bool,
    CANTIDAD integer
);
DROP TABLE IF EXISTS traslados_covid_19;
CREATE TABLE IF NOT EXISTS traslados_covid_19 ( 
    N_TRABAJO integer,
    FECHA varchar(20),
    TIPO_TRASLADO varchar(15),
    TIPO_TRANSPORTE varchar(10),
    OFICINA varchar(30),
    CESAC varchar(40),
    RECORRIDO varchar(20)
);
DROP TABLE IF EXISTS reporte_covid_sitio_gobierno;
CREATE TABLE IF NOT EXISTS reporte_covid_sitio_gobierno ( 
    FECHA varchar(20),
    TIPO_REPORTE varchar(20),
    TIPO_DATO varchar(80),
    SUBTIPO_DATO varchar(60),
    VALOR float,
    FECHA_PROCESO varchar(20),
    ID_CARGA integer
);
DROP TABLE IF EXISTS contactos_boti_triage_covid_19;
CREATE TABLE IF NOT EXISTS contactos_boti_triage_covid_19 ( 
    fecha varchar(20),
    hora integer,
    triage_cantidad integer
);
DROP TABLE IF EXISTS casos_covid19;
CREATE TABLE IF NOT EXISTS casos_covid19 ( 
    numero_de_caso integer,
    fecha_apertura_snvs varchar(30),
    fecha_toma_muestra varchar(30),
    fecha_clasificacion varchar(30),
    provincia varchar(20),
    barrio varchar(30),
    comuna varchar(5),
    genero varchar(15),
    edad varchar(5),
    clasificacion varchar(15),
    fecha_fallecimiento  varchar(30),
    fallecido varchar(5),
    fecha_alta varchar(30),
    tipo_contagio varchar(5)
);

