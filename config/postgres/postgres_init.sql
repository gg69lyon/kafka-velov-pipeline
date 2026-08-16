-- Active l'extension PostGIS pour la gestion de la géolocalisation
CREATE EXTENSION IF NOT EXISTS postgis;

-- Crée la table des alertes si elle n'existe pas déjà
CREATE TABLE IF NOT EXISTS velov_station_alerts (
    id SERIAL PRIMARY KEY,
    alert_id VARCHAR(100) UNIQUE NOT NULL,
    alert_type VARCHAR(100) NOT NULL,
    alert_message TEXT,
    station_number INT NOT NULL,
    name VARCHAR(255) NOT NULL,
    commune VARCHAR(255),
    status VARCHAR(50),
    bikes_available INT,
    docks_available INT,
    lat DOUBLE PRECISION,
    lng DOUBLE PRECISION,
    geom GEOMETRY(Point, 4326),
    processed_at TIMESTAMP WITH TIME ZONE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Crée les index spatiaux et temporels
CREATE INDEX IF NOT EXISTS idx_velov_alerts_geom ON velov_station_alerts USING GIST (geom);
CREATE INDEX IF NOT EXISTS idx_velov_alerts_created_at ON velov_station_alerts (created_at);