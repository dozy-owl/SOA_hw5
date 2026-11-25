## 1. Kafka topic со схемой событий
Ключ партиционирования – `user_id`. Тогда осбытия одного `event_type` всегда в одной патриции, порядок `event_type` строго контролируется.

## 3

Помесячные партиции удобно удалять и архивировать, а сортировка по пользователю ускоряет запросы retention и воронки по сессиям

Проверка:

docker-compose down -v && docker-compose up -d
sleep 60
docker exec clickhouse clickhouse-client --query "SHOW TABLES FROM movie_analytics"
curl -X POST "http://localhost:8000/generator/start?duration_seconds=15&users_count=5"
sleep 25
docker exec clickhouse clickhouse-client --query "SELECT count() FROM movie_analytics.movie_events"

## 4
docker-compose build test-runner
docker-compose run --rm test-runner
