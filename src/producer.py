import json
import time
import requests
from confluent_kafka import Producer

# Configuration Kafka
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC_NAME = "velov-raw-status"

# URL API Grand Lyon
API_URL = "https://download.data.grandlyon.com/ws/grandlyon/pvo_patrimoine_voirie.pvostationvelov/all.json?maxrecords=-1"

def delivery_report(err, msg):
    if err is not None:
        print(f"Erreur Kafka : {err}")
    else:
        print(f"Station {msg.key().decode('utf-8')} envoyée -> Topic: {msg.topic()} [Partition: {msg.partition()}]")

def fetch_and_format_velov_data():
    """Récupère et associe les 'fields' avec les 'values' de l'API Grand Lyon."""
    try:
        response = requests.get(API_URL, timeout=10)
        response.raise_for_status()
        data = response.json()
        
        fields = data.get("fields", [])
        values_list = data.get("values", [])
        
        if not fields or not values_list:
            print("Fichier JSON reçu mais 'fields' ou 'values' est vide.")
            return []

        formatted_stations = []
        for raw_values in values_list:
            # Associe chaque nom de champ à sa valeur (ex: {'idstation': 10001, 'name': 'St Paul', ...})
            station_dict = dict(zip(fields, raw_values))
            formatted_stations.append(station_dict)
            
        return formatted_stations

    except Exception as e:
        print(f"Erreur lors de la récupération des données : {e}")
        return []

def main():
    producer_config = {
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'client.id': 'velov-producer-lyon'
    }
    
    producer = Producer(producer_config)
    print(f"Démarrage du Producer Kafka Lyon pour le topic '{TOPIC_NAME}'...")
    
    while True:
        stations = fetch_and_format_velov_data()
        print(f"\n{len(stations)} stations Vélo'v formatées et prêtes à l'envoi...")

        if not stations:
            print("Aucune station récupérée. Retentative dans 10s...")
            time.sleep(10)
            continue

        for station in stations:
            # Récupération de l'ID station (souvent nommé 'number' ou 'idstation')
            station_id = station.get("number") or station.get("idstation") or station.get("gid")
            
            if not station_id:
                continue

            # Création du message JSON propre
            payload = {
                "station_number": station_id,
                "name": station.get("name") or station.get("nom"),
                "status": station.get("status") or station.get("etat"),
                "bikes_available": station.get("bikes", 0) or station.get("velos", 0),
                "docks_available": station.get("bike_stands", 0) or station.get("places", 0),
                "commune": station.get("commune"),
                "lat": station.get("lat") or station.get("latitude"),
                "lng": station.get("lng") or station.get("longitude"),
                "timestamp": time.time()
            }

            # Envoi dans Kafka
            producer.produce(
                topic=TOPIC_NAME,
                key=str(station_id),
                value=json.dumps(payload),
                on_delivery=delivery_report
            )
            producer.poll(0)

        producer.flush()
        print("Envoi terminé ! Prochain rafraîchissement dans 30 secondes...")
        time.sleep(30)

if __name__ == "__main__":
    main()