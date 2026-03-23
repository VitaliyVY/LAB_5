# Лабораторна робота №5

## Тема роботи
Побудова системи обміну повідомленнями на базі Apache Kafka: продюсер, два топіки, два споживачі, запуск у Docker.

## Мета роботи
1. Створити Python-проєкт `producer`.
2. Зчитувати дані з CSV-файлу та формувати подію для кожного запису.
3. Публікувати кожну подію у два топіки: `Topic1` і `Topic2`.
4. Розгорнути Kafka-кластер у Docker:
- `Zookeeper`
- `Broker1`
- `Broker2`
- `Kafka UI`
5. Створити два Python-проєкти споживачів:
- `consumer1` для `Topic1`
- `consumer2` для `Topic2`
6. Запускати всі компоненти через Docker Compose.
7. Підготувати проєкт до здачі через GitHub.

## Використані технології
- Python 3.11
- Бібліотека `kafka-python`
- Docker, Docker Compose
- Apache Kafka (Confluent images)
- Kafka UI (Provectus)

## Структура проєкту
```text
Lab_5/
├─ docker-compose.yml
├─ Divvy_Trips_2019_Q4.csv
├─ .gitignore
├─ README.md
├─ producer/
│  ├─ Dockerfile
│  ├─ requirements.txt
│  └─ producer.py
├─ consumer1/
│  ├─ Dockerfile
│  ├─ requirements.txt
│  └─ consumer.py
└─ consumer2/
   ├─ Dockerfile
   ├─ requirements.txt
   └─ consumer.py
```

## Склад Kafka-кластера
- `zookeeper`
- `broker1`
- `broker2`
- `kafka-init` (автоматично створює `Topic1` і `Topic2`)
- `kafka-ui` (інтерфейс: http://localhost:8080)

## Принцип роботи
1. `producer` читає файл `Divvy_Trips_2019_Q4.csv`.
2. Для кожного рядка створюється JSON-подія з полями:
- `event_id`
- `timestamp`
- `payload` (вміст рядка CSV)
3. Кожна подія відправляється одночасно в `Topic1` і `Topic2`.
4. `consumer1` читає повідомлення з `Topic1` і виводить у консоль.
5. `consumer2` читає повідомлення з `Topic2` і виводить у консоль.

## Налаштування (env у Docker Compose)
- `KAFKA_BOOTSTRAP_SERVERS=broker1:9092,broker2:9092`
- `KAFKA_TOPIC_1=Topic1`
- `KAFKA_TOPIC_2=Topic2`
- `KAFKA_TOPIC=Topic1` або `Topic2` для споживачів
- `MAX_RECORDS=200` (для демонстрації, щоб не відправляти весь CSV)
- `SEND_DELAY_SECONDS=0.2`

## Запуск
```bash
docker compose up --build
```

Запуск у фоновому режимі:
```bash
docker compose up -d --build
```

## Перевірка результату
1. Логи продюсера:
```bash
docker compose logs --tail=100 producer
```
Очікувано: `Sent event_id=... to Topic1 and Topic2`, потім `Completed. Total events sent: ...`

2. Логи першого споживача:
```bash
docker compose logs --tail=100 consumer1
```
Очікувано: повідомлення з `Topic1`.

3. Логи другого споживача:
```bash
docker compose logs --tail=100 consumer2
```
Очікувано: повідомлення з `Topic2`.

4. Kafka UI (`http://localhost:8080`):
- кластер `lab5-cluster` у стані `Online`
- `Brokers count = 2`
- наявні топіки `Topic1` і `Topic2`
- у топіках є повідомлення

## Зупинка проєкту
```bash
docker compose down
```

## Публікація в GitHub
```bash
git init
git add .
git commit -m "Lab5: Kafka producer and consumers with Docker"
git branch -M main
git remote add origin https://github.com/VitaliyVY/LAB_5.git
git push -u origin main
```

## Що прикріпити в Moodle
1. Посилання на GitHub-репозиторій.
2. Скріншот Kafka UI (кластер online, 2 брокери, топіки).
3. Скріншоти логів `producer`, `consumer1`, `consumer2`.

## Висновок
У лабораторній роботі реалізовано повний цикл обробки подій у Kafka: зчитування даних з CSV, публікація подій у два топіки, споживання цих подій двома окремими споживачами та контейнеризований запуск усіх компонентів через Docker Compose.
