import os
import json
import datetime
import psycopg2
from confluent_kafka import Consumer, KafkaError

# Configuration via variables d'environnement (avec valeurs par défaut)
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_DB = os.getenv("POSTGRES_DB", "velov_db")
POSTGRES_USER = os.getenv("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "postgres_password")

TOPIC_NAME = "velov-station-alerts"
GROUP_ID = "velov-db-consumer-group"


def get_db_connection():
    """Établit la connexion à la base de données PostgreSQL."""
    return psycopg2.connect(
        host=POSTGRES_HOST,
        database=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
        port=5432
    )


def init_db(conn):
    """Initialise l'extension PostGIS et la structure de la table."""
    with conn.cursor() as cur:
        cur.execute("CREATE EXTENSION IF NOT EXISTS postgis;")
        cur.execute("""
            CREATE TABLE IF NOT EXISTS velov_station_alerts (
                id SERIAL PRIMARY KEY,
                alert_id VARCHAR(100) UNIQUE NOT NULL,
                alert_type VARCHAR(50) NOT NULL,
                alert_message TEXT,
                station_number INT NOT NULL,
                name VARCHAR(255),
                commune VARCHAR(255),
                bikes_available INT,
                docks_available INT,
                lat DOUBLE PRECISION,
                lng DOUBLE PRECISION,
                geom GEOMETRY(Point, 4326),
                processed_at TIMESTAMP WITH TIME ZONE,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
            );
            
            -- Garantit la présence des colonnes et de la contrainte UNIQUE
            ALTER TABLE velov_station_alerts ADD COLUMN IF NOT EXISTS alert_id VARCHAR(100);
            CREATE UNIQUE INDEX IF NOT EXISTS idx_unique_alert_id ON velov_station_alerts (alert_id);

            -- Index spatiaux et temporels
            CREATE INDEX IF NOT EXISTS idx_velov_alerts_geom ON velov_station_alerts USING GIST (geom);
            CREATE INDEX IF NOT EXISTS idx_velov_alerts_created_at ON velov_station_alerts (created_at);
        """)
        conn.commit()


def save_alert_to_db(conn, alert):
    """Convertit les types et insère un enregistrement d'alerte dans PostgreSQL avec sa géométrie PostGIS."""
    try:
        lat = float(alert.get("lat") or 0.0)
        lng = float(alert.get("lng") or 0.0)

        # Conversion du timestamp Unix (float/int) en objet datetime UTC
        raw_processed_at = alert.get("processed_at")
        processed_at_dt = None
        if raw_processed_at is not None:
            try:
                processed_at_dt = datetime.datetime.fromtimestamp(
                    float(raw_processed_at), tz=datetime.timezone.utc
                )
            except (ValueError, TypeError):
                processed_at_dt = None

        query = """
            INSERT INTO velov_station_alerts (
                alert_id, alert_type, alert_message, station_number,
                name, commune, bikes_available, docks_available,
                lat, lng, geom, processed_at
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, ST_SetSRID(ST_MakePoint(%s, %s), 4326), %s)
            ON CONFLICT (alert_id) DO NOTHING;
        """

        with conn.cursor() as cur:
            cur.execute(query, (
                alert.get("alert_id"),
                alert.get("alert_type"),
                alert.get("alert_message"),
                alert.get("station_number"),
                alert.get("name"),
                alert.get("commune"),
                alert.get("bikes_available", 0),
                alert.get("docks_available", 0),
                lat,
                lng,
                lng,  # X (Longitude) pour ST_MakePoint
                lat,  # Y (Latitude) pour ST_MakePoint
                processed_at_dt
            ))
            conn.commit()
            print(f"[DB] Alerte sauvegardée : {alert.get('alert_type')} | Station: {alert.get('station_number')}")

    except Exception as e:
        conn.rollback()
        print(f"Erreur lors de la sauvegarde : {e}")


def main():
    conn = get_db_connection()
    init_db(conn)

    consumer = Consumer({
        'bootstrap.servers': KAFKA_BOOTSTRAP,
        'group.id': GROUP_ID,
        'auto.offset.reset': 'earliest'
    })
    consumer.subscribe([TOPIC_NAME])

    print(f"[Consumer DB] Écoute du topic '{TOPIC_NAME}' pour insertion PostGIS...")

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    print(f"Erreur Consumer : {msg.error()}")
                    break

            try:
                alert = json.loads(msg.value().decode('utf-8'))
                save_alert_to_db(conn, alert)
            except Exception as e:
                print(f"Erreur de décodage du message : {e}")

    except KeyboardInterrupt:
        print("\nArrêt du Consumer DB.")
    finally:
        consumer.close()
        conn.close()


if __name__ == "__main__":
    main()