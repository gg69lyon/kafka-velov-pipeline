import json
import time
import requests
from confluent_kafka import Producer

# Configuration Kafka
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC_NAME = "velov-raw-status"

# URL API Temps Réel Métropole de Lyon (Vélo'v)
API_URL = 'https://download.data.grandlyon.com/ws/rdata/jcd_jcdecaux.jcdvelov/all.json'

def delivery_report(err, msg):
    """Callback de confirmation d'envoi vers Kafka."""
    if err is not None:
        print(f"Échec de la livraison : {err}")
    else:
        print(f"[Vélo'v] Message envoyé à {msg.topic()}")

def fetch_velov_data():
    """Récupère l'état temps réel des stations Vélo'v à Lyon."""
    try:
        response = requests.get(API_URL, timeout=10)
        response.raise_for_status()
        data = response.json()
        # L'API du Grand Lyon renvoie la liste des stations dans le champ 'values'
        return data.get("values", [])
    except Exception as e:
        print(f"Erreur lors de l'appel à l'API Grand Lyon : {e}")
        return []

def main():
    producer_config = {
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'client.id': 'velov-producer-lyon'
    }
    producer = Producer(producer_config)

    print(f"Démarrage du Producer Kafka Lyon pour le topic '{TOPIC_NAME}'...")

    try:
        while True:
            stations = fetch_velov_data()
            print(f"\nRécupération de {len(stations)} stations Vélo'v (Lyon)...")

            for station in stations:
                print(station)
                # Structuration des données au format JSON
                payload = {
                    "station_number": station.get("number"),
                    "name": station.get("name"),
                    "address": station.get("address"),
                    "status": station.get("status"),
                    "bikes_available": station.get("available_bikes"),
                    "docks_available": station.get("available_bike_stands"),
                    "total_stands": (station.get("available_bikes", 0) or 0) + (station.get("available_bike_stands", 0) or 0),
                    "lat": station.get("lat"),
                    "lng": station.get("lng"),
                    "commune": station.get("commune"),
                    "availability_code": station.get("availability"),
                    "timestamp": time.time()
                }

                print(f"Payload: {payload}")

                # La clé de partitionnement = le numéro de station (ex: "10001")
                key = str(payload["station_number"])

                # Envoi dans Kafka
                producer.produce(
                    topic=TOPIC_NAME,
                    key=key,
                    value=json.dumps(payload),
                    on_delivery=delivery_report
                )

                producer.poll(0)

            producer.flush()
            print("Pause de 30 secondes avant le prochain rafraîchissement...")
            time.sleep(30)

    except KeyboardInterrupt:
        print("\nArrêt du Producer Vélo'v.")
    finally:
        producer.flush()

if __name__ == "__main__":
    main()