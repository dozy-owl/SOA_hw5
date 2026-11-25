import os
import uuid
import time
import requests
from clickhouse_driver import Client


PRODUCER_URL = os.getenv("PRODUCER_URL", "http://producer:8000/events")
CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "clickhouse")

def get_clickhouse_client():
    return Client(host=CLICKHOUSE_HOST, port=9000, user="default", password="", database="movie_analytics")

def test_event_pipeline():
    test_event_id = str(uuid.uuid4())
    test_user_id = f"test_user_{uuid.uuid4().hex[:6]}"
    
    payload = {
        "event_id": test_event_id,
        "user_id": test_user_id,
        "movie_id": "test_movie_001",
        "event_type": "VIEW_STARTED",
        "device_type": "DESKTOP",
        "session_id": f"test_session_{uuid.uuid4().hex[:6]}",
        "progress_seconds": 0
    }

    response = requests.post(PRODUCER_URL, json=payload, timeout=5)
    assert response.status_code == 200, f"Producer failed: {response.text}"
    assert response.json()["status"] == "success"

    client = get_clickhouse_client()
    
    max_wait_seconds = 30
    deadline = time.time() + max_wait_seconds
    while time.time() < deadline:
        result = client.execute(
            "SELECT event_id, user_id, event_type, movie_id, device_type, session_id, progress_seconds FROM movie_events WHERE event_id = %(event_id)s",
            {"event_id": test_event_id}
        )
        if result:
            row = result[0]
            assert row[0] == test_event_id, "event_id mismatch"
            assert row[1] == test_user_id, "user_id mismatch"
            assert row[2] == "VIEW_STARTED", "event_type mismatch"
            assert row[3] == "test_movie_001", "movie_id mismatch"
            assert row[4] == "DESKTOP", "device_type mismatch"
            assert row[5] == payload["session_id"], "session_id mismatch"
            assert row[6] == 0, "progress_seconds mismatch"
            client.execute(
                "ALTER TABLE movie_events DELETE WHERE user_id = %(u)s",
                {"u": test_user_id},
                settings={"mutations_sync": 1},
            )
            return
        time.sleep(0.5)

    assert False, f"Событие не появилось в ClickHouse за {max_wait_seconds} с"