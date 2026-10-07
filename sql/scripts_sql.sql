
#Tabla en la base de datos workshop2_etl

## para importar el .csv de grammys_awards.csv a la tabla Base de datos workshop2_etl, se puede usar el siguiente comando en psql:

CREATE TABLE grammy_awards (
    year         INTEGER,
    title        TEXT,
    published_at TIMESTAMPTZ,
    updated_at   TIMESTAMPTZ,
    category     TEXT,
    nominee      TEXT,
    artist       TEXT,
    workers      TEXT,
    img          TEXT,
    winner       BOOLEAN
);


## base de datos artistas_spotify_grammys (merge de artistas de spotify y grammy_awards)

CREATE TABLE IF NOT EXISTS artistas_spotify_grammys (
    artista_key          TEXT PRIMARY KEY,
    artista              TEXT NOT NULL,
    en_spotify           BOOLEAN NOT NULL,
    tiene_grammy         BOOLEAN NOT NULL,
    n_canciones          INTEGER NOT NULL,
    genero_principal     TEXT,
    popularidad_prom     DOUBLE PRECISION,
    duracion_min_prom    DOUBLE PRECISION,
    prop_explicit        DOUBLE PRECISION,
    danceability_prom    DOUBLE PRECISION,
    energy_prom          DOUBLE PRECISION,
    valence_prom         DOUBLE PRECISION,
    acousticness_prom    DOUBLE PRECISION,
    tempo_prom           DOUBLE PRECISION,
    premios_grammy       INTEGER NOT NULL,
    primer_anio          INTEGER,
    ultimo_anio          INTEGER,
    categorias_distintas INTEGER NOT NULL
);
