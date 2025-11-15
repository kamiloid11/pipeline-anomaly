# Pipeline Anomaly Detection

Этот проект представляет собой упрощённый, но максимально понятный пайплайн для обработки временных рядов, детекции аномалий и загрузки результатов в ClickHouse. Он полностью Docker‑изолирован, легко запускается и расширяется.

## 📌 Возможности
- Загрузка CSV-файлов с временными рядами
- Простейшее правило детекции аномалий (z-score)
- Подсчёт базовых метрик: количество строк, число аномалий, процент аномалий
- Логирование
- Сохранение результатов в ClickHouse
- Удобный CLI-интерфейс

---

## 📂 Структура проекта
```
pipeline-anomaly/
│
├── docker-compose.yml          # Оркестрация ClickHouse + pipeline
├── Dockerfile                  # Образ для pipeline
├── requirements.txt            # Python-зависимости
│
├── pipeline/
│   ├── __init__.py
│   ├── runner.py               # CLI входная точка
│   ├── processor.py            # Логика обработки CSV
│   ├── anomaly.py              # Логика детекции аномалий
│   ├── storage.py              # Запись в ClickHouse
│   └── config.py               # Конфиг + параметры
│
└── data/
    └── sample.csv              # Пример данных
```

---

## ⚙️ Требования
- Docker
- Docker Compose
- Git (если клонируете репозиторий)

---

## 🚀 Установка и запуск

### 1. Клонируйте репозиторий
```
git clone https://github.com/<your-name>/pipeline-anomaly.git
cd pipeline-anomaly
```

### 2. Постройте и запустите инфраструктуру
```
docker-compose up -d --build
```
Проверить ClickHouse:
```
curl -s "http://localhost:8123/?query=SELECT+1" -u pipeline:pipeline_pass
```
Ожидаемый вывод:
```
1
```

### 3. Запустите пайплайн на CSV
```
docker-compose run --rm pipeline python -m pipeline.runner run --csv /app/data/sample.csv
```
Ожидаемый вывод примерно такой:
```
INFO Processing file: /app/data/sample.csv
INFO rows_processed=5 anomalies=0 anomaly_rate=0.0000
INFO Load to ClickHouse
```

---

## 🗃 Проверка записей в ClickHouse
Показать базы:
```
docker exec -it clickhouse clickhouse-client --user pipeline --password pipeline_pass --query "SHOW DATABASES"
```
Показать таблицы:
```
docker exec -it clickhouse clickhouse-client --user pipeline --password pipeline_pass --query "SHOW TABLES FROM pipeline"
```
Смотреть данные:
```
docker exec -it clickhouse clickhouse-client --user pipeline --password pipeline_pass --query "SELECT * FROM pipeline.timeseries LIMIT 10"
```

---

## 🔍 Логика детекции аномалий
Используется простая модель Z-score:
- считаем среднее
- считаем стандартное отклонение
- точка = аномалия, если `abs((x - mean) / std) > threshold`

Порог по умолчанию: **3.0**.

---

## 🧩 Расширение проекта
Можно легко добавить:
- новые методы детекции аномалий
- REST API поверх pipeline
- Kafka + стриминг вместо файлов
- Grafana для визуализации данных в ClickHouse

---

## ✨ Пример данных (`data/sample.csv`)
```
timestamp,value
2023-01-01 00:00:00,10
2023-01-01 00:01:00,11
2023-01-01 00:02:00,9
2023-01-01 00:03:00,10
2023-01-01 00:04:00,10
```