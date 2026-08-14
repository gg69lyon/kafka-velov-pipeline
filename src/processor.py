import json
import time
from confluent_kafka import Consumer, Producer, KafkaError

# Configuration Kafka
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
INPUT_TOPIC = "velov-raw-status"
OUTPUT_TOPIC = "velov-station-alerts"
CONSUMER_GROUP_ID = "velov-alert-processor-group"

def delivery_report(err, msg):
    if err is not None:
        print(f"Erreur lors de la publication de l'alerte : {err}")
    else:
        print(f"[ALERTE PUBLIÉE] _ Topic: {msg.topic()} | Station: {msg.key().decode('utf-8')}")

def create_alert_payload(station_data, alert_type, alert_message):
    """Enrichit le message original avec des informations d'alerte."""
    return {
        "alert_id": f"{station_data.get('station_number')}_{int(time.time())}",
        "alert_type": alert_type,  # 'EMPTY_STATION', 'LOW_STOCK' ou 'FULL_STATION'
        "alert_message": alert_message,
        "station_number": station_data.get("station_number"),
        "name": station_data.get("name"),
        "commune": station_data.get("commune"),
        "bikes_available": station_data.get("bikes_available"),
        "docks_available": station_data.get("docks_available"),
        "lat": station_data.get("lat"),
        "lng": station_data.get("lng"),
        "processed_at": time.time()
    }

def process_station_data(station):
    """Applique les règles métier sur les données d'une station et renvoie un payload d'alerte ou None."""
    station_name = station.get("name", "Inconnue")
    bikes = station.get("bikes_available", 0)
    docks = station.get("docks_available", 0)
    status = station.get("status", "OPEN")

    # Ne traiter que les stations ouvertes
    if status != "OPEN":
        return None

    # Règle 1 : Station totalement vide (Pénurie)
    if bikes == 0:
        return create_alert_payload(
            station,
            alert_type="EMPTY_STATION",
            alert_message=f"Rupture de stock ! 0 vélo disponible à {station_name}."
        )
    
    # Règle 2 : Stock faible
    elif bikes <= 15:
        return create_alert_payload(
            station,
            alert_type="LOW_STOCK",
            alert_message=f"Stock faible ({bikes} vélos) à {station_name}."
        )
        
    # Règle 3 : Station totalement pleine (Saturation)
    elif docks == 0:
        return create_alert_payload(
            station,
            alert_type="FULL_STATION",
            alert_message=f"Station saturée ! 0 place disponible à {station_name}."
        )

    return None


def main():
    consumer_config = {
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'group.id': CONSUMER_GROUP_ID,
        'auto.offset.reset': 'earliest'
    }
    consumer = Consumer(consumer_config)
    consumer.subscribe([INPUT_TOPIC])

    producer_config = {
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'client.id': 'velov-alert-producer'
    }
    producer = Producer(producer_config)

    print(f"[Stream Processor] Écoute du topic '{INPUT_TOPIC}'...")
    print(f"Les alertes seront redirigées vers '{OUTPUT_TOPIC}'...\n")

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
                station = json.loads(msg.value().decode('utf-8'))
            except Exception as e:
                print(f"Impossible de décoder le JSON : {e}")
                continue

            # Traitement métier
            alert_payload = process_station_data(station)

            if alert_payload:
                key = str(station.get("station_number"))
                producer.produce(
                    topic=OUTPUT_TOPIC,
                    key=key,
                    value=json.dumps(alert_payload),
                    on_delivery=delivery_report
                )
                producer.poll(0)

    except KeyboardInterrupt:
        print("\nArrêt du Stream Processor.")
    finally:
        consumer.close()
        producer.flush()

if __name__ == "__main__":
    main()