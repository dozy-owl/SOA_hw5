from fastapi import FastAPI, HTTPException
import os
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka import Producer
from pydantic import BaseModel, Field
import uuid
import asyncio
import fastavro
import json
import time
import random
from datetime import datetime, timezone
from confluent_kafka.serialization import SerializationContext, MessageField


app = FastAPI(title="movie-events producer")

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka-1:29092,kafka-2:29094,kafka-3:29096")
SCHEMA_REGISTRY_URL = os.getenv("SCHEMA_REGISTRY_URL", "http://schema-registry:8081")
TOPIC_NAME = "movie-events"

with open("/app/movie-events.avsc", "r") as f:
    parsed_schema = fastavro.parse_schema(json.load(f))
    schema_str = json.dumps(parsed_schema)

schema_registry_client = SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})
avro_serializer = AvroSerializer(schema_registry_client, schema_str)

producer = Producer({
    "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
    "acks": "all",
    "retries": 5,
    "retry.backoff.ms": 100,
})

class EventInp(BaseModel):
    event_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    user_id: str
    movie_id: str
    event_type: str
    timestamp: int = Field(default_factory=lambda: int(datetime.now(timezone.utc).timestamp() * 1000))
    device_type: str
    session_id: str
    progress_seconds: int = 0

def delivery_report(err, _):
    if err:
        print(f"Сбой доставки: {err}")

def publish_event(event_data):
    fastavro.validation.validate(event_data, parsed_schema)
    
    key = event_data["user_id"].encode("utf-8")
    
    producer.produce(
        topic=TOPIC_NAME,
        key=key,
        value=avro_serializer(event_data, SerializationContext(TOPIC_NAME, MessageField.VALUE)),
        on_delivery=delivery_report
    )
    producer.poll(0)

@app.post("/events")
async def create_event(event: EventInp):
    event_data = event.model_dump()
    try:
        publish_event(event_data)
        print(f"PUBLISHED: event_id={event_data['event_id']}, event_type={event_data['event_type']}, timestamp={event_data['timestamp']}")
        return {"status": "success", "event_id": event_data["event_id"]}
    except fastavro.validation.ValidationError as e:
        raise HTTPException(status_code=400, detail=f"Schema validation failed: {e}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/generator/start")
async def start_generator(duration_seconds: int = 10, users_count: int = 5):
    asyncio.create_task(generate_event(duration_seconds, users_count))
    return {"status": "generator_started"}

async def generate_event(duration_seconds, users_count):
    try:
        end_time = time.time() + duration_seconds
        devices = [
                    "MOBILE",
                    "DESKTOP",
                    "TV",
                    "TABLET"
                ]
        movies = [f"movie_{i}" for i in range(1, 21)]

        active_sessions = {}
        while time.time() < end_time:
            for _ in range(random.randint(1, 3)):
                user_id = f"user_{random.randint(1, users_count)}"
                session_id = str(uuid.uuid4())
                movie_id = random.choice(movies)

                active_sessions[session_id] = {
                    "user_id": user_id,
                    "movie_id": movie_id,
                    "progress": 0
                }
                publish_event({
                    "event_id": str(uuid.uuid4()), 
                    "user_id": user_id, 
                    "movie_id": movie_id,
                    "event_type": "VIEW_STARTED", 
                    "timestamp": int(time.time() * 1000),
                    "device_type": random.choice(devices),
                    "session_id": session_id,
                    "progress_seconds": 0
                })

            to_remove = []
            for session_id, data in active_sessions.items():
                action = random.choices(
                    ["VIEW_PAUSED", "VIEW_RESUMED", "VIEW_FINISHED", "LIKED", "SEARCHED"],
                    weights=[10, 10, 30, 5, 5]
                )[0]

                if action == "VIEW_FINISHED":
                    data["progress"] += random.randint(60, 300)
                    to_remove.append(session_id)
                elif action in ["VIEW_PAUSED", "VIEW_RESUMED"]:
                    data["progress"] += random.randint(10, 60)

                publish_event({
                    "event_id": str(uuid.uuid4()), 
                    "user_id": data["user_id"], 
                    "movie_id": data["movie_id"],
                    "event_type": action, 
                    "timestamp": int(time.time() * 1000),
                    "device_type": random.choice(devices), 
                    "session_id": session_id, 
                    "progress_seconds": data["progress"]
                })

            for elem in to_remove:
                active_sessions.pop(elem)

            await asyncio.sleep(1)
    except Exception as e:
        print(f"Generator failed: {e}")
        raise