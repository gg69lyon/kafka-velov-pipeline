import pytest
from src.processor import process_station_data, create_alert_payload

@pytest.fixture
def base_station():
    """Fixture renvoyant une station modèle normale."""
    return {
        "station_number": 1001,
        "name": "Bellecour",
        "commune": "Lyon 2ème",
        "status": "OPEN",
        "bikes_available": 20,
        "docks_available": 10,
        "lat": 45.7578,
        "lng": 4.8320
    }

def test_empty_station_alert(base_station):
    """Teste le déclenchement de l'alerte EMPTY_STATION quand 0 vélo est disponible."""
    base_station["bikes_available"] = 0
    base_station["docks_available"] = 15

    alert = process_station_data(base_station)

    assert alert is not None
    assert alert["alert_type"] == "EMPTY_STATION"
    assert "Rupture de stock" in alert["alert_message"]
    assert alert["station_number"] == 1001

def test_low_stock_alert(base_station):
    """Teste le déclenchement de l'alerte LOW_STOCK quand le nombre de vélos est <= 15."""
    base_station["bikes_available"] = 5
    base_station["docks_available"] = 10

    alert = process_station_data(base_station)

    assert alert is not None
    assert alert["alert_type"] == "LOW_STOCK"
    assert "Stock faible (5 vélos)" in alert["alert_message"]

def test_full_station_alert(base_station):
    """Teste le déclenchement de l'alerte FULL_STATION quand 0 place est disponible."""
    base_station["bikes_available"] = 25
    base_station["docks_available"] = 0

    alert = process_station_data(base_station)

    assert alert is not None
    assert alert["alert_type"] == "FULL_STATION"
    assert "Station saturée" in alert["alert_message"]

def test_no_alert_for_normal_station(base_station):
    """Vérifie qu'aucune alerte n'est générée pour une station dans un état normal."""
    base_station["bikes_available"] = 20
    base_station["docks_available"] = 10

    alert = process_station_data(base_station)

    assert alert is None

def test_closed_station_ignored(base_station):
    """Vérifie que les stations fermées (CLOSED) sont ignorées même si 0 vélo."""
    base_station["status"] = "CLOSED"
    base_station["bikes_available"] = 0

    alert = process_station_data(base_station)

    assert alert is None

def test_create_alert_payload_structure(base_station):
    """Vérifie la présence de tous les champs obligatoires dans le payload d'alerte."""
    payload = create_alert_payload(base_station, "TEST_ALERT", "Message de test")

    expected_keys = [
        "alert_id", "alert_type", "alert_message", "station_number",
        "name", "commune", "bikes_available", "docks_available",
        "lat", "lng", "processed_at"
    ]
    for key in expected_keys:
        assert key in payload