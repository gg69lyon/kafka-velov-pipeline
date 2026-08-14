-- Activation de l'extension PostGIS
CREATE EXTENSION IF NOT EXISTS postgis;

-- Création de la table avec type de données géospatial
CREATE TABLE IF NOT EXISTS velov_station_alerts (
    alert_id VARCHAR(255) PRIMARY KEY,
    alert_type VARCHAR(50) NOT NULL,
    alert_message TEXT NOT NULL,
    station_number INT NOT NULL,
    name VARCHAR(255),
    commune VARCHAR(255),
    bikes_available INT,
    docks_available INT,
    lat FLOAT,
    lng FLOAT,
    geom GEOMETRY(Point, 4326),  -- Colonne géospatiale (SRID 4326 = WGS 84)
    processed_at DOUBLE PRECISION NOT NULL
);

-- Index spatial pour optimiser les requêtes géographiques
CREATE INDEX IF NOT EXISTS idx_velov_alerts_geom ON velov_station_alerts USING GIST (geom);