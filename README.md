# Лабораторна робота №5 (Kafka Streams, Java)

## Тема роботи
Потокова обробка даних поїздок у Kafka Streams з агрегуванням за датою поїздки.

## Виконані етапи
1. Створено Java-проєкт `streams-app` (Maven).
2. Додано залежності для Kafka Streams (`org.apache.kafka:kafka-streams`).
3. Реалізовано підписку на топік попередньої лабораторної (`Topic1`) і обчислення:
   - середня тривалість поїздки на день;
   - кількість поїздок на день;
   - найпопулярніша початкова станція на день;
   - топ-3 станції на день (враховано і `from_station_name`, і `to_station_name`).
4. Кожен результат відправляється в окремий Kafka-топік.

## Технології
- Java 17
- Apache Kafka Streams
- Maven
- Docker, Docker Compose
- Apache Kafka (Confluent)
- Kafka UI
- Apache Iceberg
- Apache Polaris
- Trino
- MinIO

## Структура проєкту
```text
Lab_5/
├─ docker-compose.yml
├─ Divvy_Trips_2019_Q4.csv
├─ README.md
├─ scripts/
│  └─ setup-polaris.ps1
├─ trino/
│  ├─ catalog/
│  │  └─ iceberg.properties
│  └─ demo-iceberg.sql
├─ producer/
│  ├─ Dockerfile
│  ├─ requirements.txt
│  └─ producer.py
├─ streams-app/
│  ├─ Dockerfile
│  ├─ pom.xml
│  └─ src/main/java/com/lab5/streaming/TripStreamsApplication.java
├─ consumer1/   (залишено з попередньої лабораторної)
└─ consumer2/   (залишено з попередньої лабораторної)
```

## Топіки
Вхідні:
- `Topic1` (використовується Kafka Streams застосунком)
- `Topic2` (залишився для сумісності з попередньою лабораторною)

Вихідні (по одному на кожне обчислення):
- `trip-avg-duration-by-day`
- `trip-count-by-day`
- `trip-top-start-station-by-day`
- `trip-top3-stations-by-day`

## Формат вхідного повідомлення
`producer` надсилає JSON виду:
```json
{
  "event_id": 1,
  "timestamp": "2026-04-12T10:00:00Z",
  "payload": {
    "start_time": "2019-10-01 00:01:39",
    "tripduration": "940.0",
    "from_station_name": "Sheffield Ave & Kingsbury St",
    "to_station_name": "Leavitt St & Armitage Ave"
  }
}
```

Агрегація виконується за датою з `payload.start_time` (`YYYY-MM-DD`).

## Налаштування Kafka Streams сервісу
Основні змінні середовища:
- `KAFKA_BOOTSTRAP_SERVERS=broker1:9092,broker2:9092`
- `STREAMS_APPLICATION_ID=lab5-trip-analytics-streams`
- `INPUT_TOPIC=Topic1`
- `AVG_DURATION_TOPIC=trip-avg-duration-by-day`
- `TRIP_COUNT_TOPIC=trip-count-by-day`
- `TOP_START_STATION_TOPIC=trip-top-start-station-by-day`
- `TOP3_STATIONS_TOPIC=trip-top3-stations-by-day`

## Запуск
```bash
docker compose up --build
```

Або у фоні:
```bash
docker compose up -d --build
```

Після запуску доступні сервіси:
- Trino Web UI: `http://localhost:8080`
- Kafka UI: `http://localhost:8088`
- MinIO UI: `http://localhost:9001` (`admin` / `password`)
- MinIO API: `http://localhost:9000`
- Polaris API: `http://localhost:8181`

## Налаштування Iceberg Lakehouse
Конфігурація Trino для Iceberg REST catalog знаходиться у файлі `trino/catalog/iceberg.properties`.

Після старту контейнерів створити каталог Polaris і ролі доступу можна командою:
```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\setup-polaris.ps1
```

Після цього можна виконати демонстраційні SQL-запити в Trino:
```powershell
docker compose exec trino trino --server localhost:8080 --catalog iceberg -f /etc/trino/demo-iceberg.sql
```

Або відкрити інтерактивну консоль:
```powershell
docker compose exec -it trino trino --server localhost:8080 --catalog iceberg
```

## Перевірка результатів
1. Переконатися, що продюсер надсилає повідомлення:
```bash
docker compose logs --tail=100 producer
```

2. Перевірити роботу Kafka Streams:
```bash
docker compose logs --tail=200 streams-processor
```

3. Відкрити Kafka UI: `http://localhost:8088` і перевірити, що в output-топіках з'являються повідомлення:
- `trip-avg-duration-by-day`
- `trip-count-by-day`
- `trip-top-start-station-by-day`
- `trip-top3-stations-by-day`

4. Перевірити Iceberg-таблицю через Trino:
```sql
SELECT * FROM iceberg.db.customers;
```

## Зупинка
```bash
docker compose down
```

## Висновок
У проєкті реалізовано потокову Java-обробку даних поїздок на Kafka Streams з агрегуванням за датою та публікацією кожної метрики в окремий Kafka-топік.
