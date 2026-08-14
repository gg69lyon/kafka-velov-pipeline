import json
import psycopg2
from confluent_kafka import Consumer, KafkaError

# Configuration Kafka & PostgreSQL
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC_NAME = "velov-station-alerts"
CONSUMER_GROUP = "postgres-saver-group"

DB_CONFIG = {
    "dbname": "velov_db",
    "user": "postgres",
    "password": "postgres_password",
    "host": "localhost",
    "port": "5432"
}

def init_db(conn):
    with conn.cursor() as cur:
        cur.execute("CREATE EXTENSION IF NOT EXISTS postgis;")
        cur.execute("""
            CREATE TABLE IF NOT EXISTS velov_station_alerts (
                id SERIAL PRIMARY KEY,
                station_number INT NOT NULL,
                name VARCHAR(255) NOT NULL,
                status VARCHAR(50),
                bikes_available INT,
                docks_available INT,
                commune VARCHAR(255),
                alert_type VARCHAR(100) NOT NULL,
                geom GEOMETRY(Point, 4326),
                created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
            );
        """)
        conn.commit()

def save_to_postgres(cursor, conn, alert):
    """Insère l'alerte dans la table PostgreSQL."""
    query = """
        INSERT INTO velov_station_alerts 
        (alert_id, alert_type, alert_message, station_number, name, commune, bikes_available, docks_available, lat, lng, processed_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (alert_id) DO NOTHING;
    """
    cursor.execute(query, (
        alert.get("alert_id"),
        alert.get("alert_type"),
        alert.get("alert_message"),
        alert.get("station_number"),
        alert.get("name"),
        alert.get("commune"),
        alert.get("bikes_available"),
        alert.get("docks_available"),
        alert.get("lat"),
        alert.get("lng"),
        alert.get("processed_at")
    ))
    conn.commit()

def main():
    # Connexion à PostgreSQL
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        print("Connexion réussie à PostgreSQL !")
    except Exception as e:
        print(f"Impossible de se connecter à PostgreSQL : {e}")
        return

    # Connexion au Consumer Kafka
    consumer = Consumer({
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'group.id': CONSUMER_GROUP,
        'auto.offset.reset': 'earliest'
    })
    consumer.subscribe([TOPIC_NAME])

    print(f"📥 Écoute du topic '{TOPIC_NAME}' pour sauvegarde en BDD...\n")

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    print(f"Erreur Kafka : {msg.error()}")
                continue

            try:
                alert = json.loads(msg.value().decode('utf-8'))
                save_to_postgres(cursor, conn, alert)
                print(f"Alerte sauvegardée en BDD : {alert.get('name')} ({alert.get('alert_type')})")
            except Exception as e:
                print(f"Erreur lors de la sauvegarde : {e}")

    except KeyboardInterrupt:
        print("\nArrêt du script de sauvegarde.")
    finally:
        cursor.close()
        conn.close()
        consumer.close()

if __name__ == "__main__":
    main()