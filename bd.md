Привет! Отличная идея — подготовиться заранее. На соревнованиях по ML время — самый ценный ресурс. Вот компактная шпаргалка по работе с базами данных, ориентированная на задачи Data Science (загрузка в pandas, быстрая выгрузка, агрегации).

---

# 🐘 PostgreSQL

### 1. Подключение
**Через терминал (psql):**
```bash
psql -U <username> -d <database_name> -h <host> -p <port>
# Пример: psql -U postgres -d mydb -h localhost -p 5432
# Ввод пароля будет запрошен автоматически
```

**Из Python (SQLAlchemy + Pandas):**
*Самый удобный способ для ML.*
```python
from sqlalchemy import create_engine
import pandas as pd

# Строка подключения
# dialect+driver://username:password@host:port/database
conn_str = "postgresql+psycopg2://user:password@localhost:5432/mydb"
engine = create_engine(conn_str)

# Чтение в DataFrame
df = pd.read_sql("SELECT * FROM table_name LIMIT 100", engine)

# Запись DataFrame в базу
df.to_sql("new_table", engine, if_exists='replace', index=False)
```

### 2. Бэкап и Восстановление
**Создание дампа (бэкап):**
```bash
# В формате SQL (текстовый)
pg_dump -U <user> -h <host> <db_name> > dump.sql

# В формате custom (сжатый, быстрее, для pg_restore)
pg_dump -U <user> -h <host> -Fc <db_name> > dump.backup
```

**Восстановление из дампа:**
```bash
# Из SQL файла
psql -U <user> -h <host> -d <db_name> < dump.sql

# Из custom файла (сначала нужно создать пустую БД)
pg_restore -U <user> -h <host> -d <db_name> dump.backup
```

### 3. Экспорт в CSV
**Через psql (команда `\copy` — права клиента, `COPY` — права сервера):**
```sql
\copy (SELECT * FROM table_name) TO 'output.csv' WITH (FORMAT csv, HEADER true, DELIMITER ',');
```

**Через Python (Pandas):**
```python
df.to_csv('output.csv', index=False)
```

### 4. Шпаргалка по SQL запросам
```sql
-- Выборка
SELECT col1, col2 FROM table WHERE col1 > 10 ORDER BY col2 DESC LIMIT 100;

-- Агрегация (группировка)
SELECT category, COUNT(*), AVG(price) 
FROM products 
GROUP BY category 
HAVING COUNT(*) > 5;

-- JOIN (соединение таблиц)
SELECT t1.id, t2.name 
FROM table1 t1 
LEFT JOIN table2 t2 ON t1.id = t2.fk_id;

-- Оконные функции (полезно для фичей)
SELECT id, value, 
       AVG(value) OVER (PARTITION BY category) as avg_cat_value 
FROM table;

-- Удаление дубликатов (оставить последний по id)
DELETE FROM table a USING table b
WHERE a.id < b.id AND a.unique_key = b.unique_key;
```

---

# 🍃 MongoDB

### 1. Подключение
**Через терминал (mongosh):**
```bash
mongosh "mongodb://<user>:<password>@<host>:<port>/<db_name>"
```

**Из Python (PyMongo + Pandas):**
```python
from pymongo import MongoClient
import pandas as pd

client = MongoClient("mongodb://user:password@localhost:27017/")
db = client['mydb']
collection = db['mycollection']

# Чтение всех документов (осторожно с большими данными!)
data = list(collection.find({}, {'_id': 0})) # Исключаем _id
df = pd.DataFrame(data)

# Чтение с фильтром
data = list(collection.find({"age": {"$gt": 18}}))
```

### 2. Бэкап и Восстановление
**Создание дампа:**
```bash
mongodump --uri="mongodb://user:password@host:port/db_name" --out=./backup_dir
```

**Восстановление:**
```bash
mongorestore --uri="mongodb://user:password@host:port/db_name" ./backup_dir/db_name
```

### 3. Экспорт в CSV
**Через терминал (mongoexport):**
```bash
mongoexport --uri="mongodb://user:password@host:port/db_name" \
--collection=collection_name \
--type=csv \
--fields=field1,field2,field3 \
--out=output.csv
```
*Важно: Если вложенные JSON объекты, лучше выгружать в JSON и разворачивать в pandas (`pd.json_normalize`).*

**Через Python (для сложных структур):**
```python
from pandas import json_normalize
data = list(collection.find())
df = json_normalize(data) # Разворачивает вложенные словари
df.to_csv('output.csv', index=False)
```

### 4. Шпаргалка по Mongo Запросам
```javascript
// Найти все
db.collection.find({})

// Найти с условием (эквивалент WHERE)
db.collection.find({ age: { $gt: 18 }, status: "active" })

// Найти конкретные поля (проекция)
db.collection.find({}, { name: 1, email: 1, _id: 0 })

// Агрегация (Pipeline) - аналог GROUP BY и JOIN
db.collection.aggregate([
  { $match: { status: "A" } },           // Фильтр
  { $group: { _id: "$cust_id", total: { $sum: "$amount" } } }, // Группировка
  { $sort: { total: -1 } },              // Сортировка
  { $limit: 10 }                         // Лимит
])

// Left Join в Mongo ($lookup)
db.orders.aggregate([
  {
    $lookup: {
      from: "products",
      localField: "product_id",
      foreignField: "_id",
      as: "product_details"
    }
  }
])
```

---

# 🚀 ML Pro-Tips (Для соревнований)

1.  **Чанки (Chunking):** Если база огромная и не влезает в RAM, читай частями.
    *   *Postgres:* `pd.read_sql(query, engine, chunksize=10000)`
    *   *Mongo:* `collection.find().batch_size(1000)`
2.  **Индексы:** Если запросы тормозят, проверь индексы.
    *   *Postgres:* `CREATE INDEX idx_name ON table(column);`
    *   *Mongo:* `db.collection.createIndex({ "field": 1 })`
3.  **EXPLAIN:** Перед запуском тяжелого запроса проверь план выполнения.
    *   *Postgres:* `EXPLAIN ANALYZE SELECT ...`
    *   *Mongo:* `db.collection.find(...).explain("executionStats")`
4.  **Временные таблицы:** В Postgres удобно создавать временные таблицы для промежуточных вычислений (`CREATE TEMP TABLE ...`). Они удаляются после сессии.
5.  **Пароли:** Не хардкодь пароли в коде, который можешь случайно запостить. Используй `.env` файлы или переменные окружения.
    ```python
    import os
    password = os.getenv("DB_PASSWORD")
    ```

Удачи на соревновании! Пусть метрики растут, а лоссы падают! 📉🚀

Конечно! Для соревнований по ML SQL нужен не просто для выгрузки, а для **Feature Engineering** (создания признаков) прямо на базе. Это экономит память и время питона.

Вот расширенная шпаргалка по SQL (с упором на **PostgreSQL**, так как он чаще всего используется в DS), разбитая по задачам.

---

# 🛠 SQL Шпаргалка для Data Science

## 1. Основы (База)
```sql
-- Выборка уникальных значений
SELECT DISTINCT category FROM products;

-- Фильтрация и сортировка
SELECT * FROM users 
WHERE age BETWEEN 18 AND 65 
  AND city IN ('Moscow', 'SPb')
  AND email IS NOT NULL
ORDER BY created_at DESC
LIMIT 1000;

-- Поиск по шаблону (LIKE)
SELECT * FROM texts WHERE content LIKE '%keyword%';
-- Регистронезависимый поиск (Postgres)
SELECT * FROM texts WHERE content ILIKE '%keyword%';
```

## 2. Joins (Объединение таблиц)
*Для ML важно понимать, как не потерять объекты при соединении.*

```sql
-- LEFT JOIN: Оставляем все строки из левой таблицы (часто это target таблица)
SELECT t1.id, t1.target, t2.feature 
FROM train_set t1
LEFT JOIN features_table t2 ON t1.id = t2.user_id;

-- INNER JOIN: Только совпадения (может уменьшить выборку!)
-- FULL JOIN: Всё подряд (редко нужно)
```

## 3. Агрегации (Group By)
*Для создания статистик по категориям (mean encoding, count encoding).*

```sql
SELECT 
    user_id,
    COUNT(*) as transaction_count,
    AVG(amount) as avg_amount,
    SUM(amount) as total_amount,
    STDDEV(amount) as std_amount, -- Важно для выбросов
    MIN(amount) as min_amount,
    MAX(amount) as max_amount
FROM transactions
GROUP BY user_id;

-- Фильтрация после группировки
HAVING COUNT(*) > 5;
```

## 4. Оконные функции (Window Functions) 🔥
*Самое важное для ML. Позволяет создавать признаки без группировки строк.*

```sql
-- Нумерация строк (для удаления дубликатов или выборки топ-N)
ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY date DESC) as rn

-- Ранжирование (с пропусками мест)
RANK() OVER (PARTITION BY category ORDER BY score DESC) as rank

-- Сдвиг во времени (Lag/Lead) - для Time Series!
-- Предыдущее значение
LAG(amount, 1) OVER (PARTITION BY user_id ORDER BY date) as prev_amount
-- Следующее значение
LEAD(amount, 1) OVER (PARTITION BY user_id ORDER BY date) as next_amount

-- Скользящее среднее (Moving Average)
AVG(amount) OVER (
    PARTITION BY user_id 
    ORDER BY date 
    ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
) as moving_avg_7days

-- Накопительный итог (Cumulative Sum)
SUM(amount) OVER (PARTITION BY user_id ORDER BY date) as cum_sum
```

## 5. Работа с NULL и Типами
```sql
-- Замена NULL на значение (например, на среднее или 0)
COALESCE(column_name, 0) as col_filled

-- Замена NULL на NULL (если пустая строка считается отсутствием)
NULLIF(column_name, '') as col_clean

-- Приведение типов (Postgres стиль ::)
SELECT 
    id::text, 
    price::numeric, 
    created_at::date 
FROM table;

-- Стандартный CAST
CAST(price AS INTEGER)
```

## 6. Условная логика (CASE WHEN)
*Для биннинга (разбиения на интервалы) и кодирования категорий.*

```sql
SELECT 
    age,
    CASE 
        WHEN age < 18 THEN 'child'
        WHEN age BETWEEN 18 AND 60 THEN 'adult'
        ELSE 'pensioner'
    END as age_group,
    CASE 
        WHEN status = 'A' THEN 1
        ELSE 0
    END as is_active_binary
FROM users;
```

## 7. Работа с Датами и Временем (Postgres)
*Критично для временных рядов и транзакционных данных.*

```sql
-- Текущая дата/время
NOW(), CURRENT_DATE, CURRENT_TIMESTAMP

-- Извлечение частей даты
EXTRACT(YEAR FROM date_col), 
EXTRACT(MONTH FROM date_col),
EXTRACT(DOW FROM date_col) -- День недели (0-6)

-- Обрезка до начала периода
DATE_TRUNC('month', date_col) -- Первое число месяца
DATE_TRUNC('hour', date_col)

-- Разница между датами
(date_col1 - date_col2) as diff_interval
EXTRACT(DAY FROM (date_col1 - date_col2)) as diff_days

-- Добавление интервала
date_col + INTERVAL '1 month'
date_col - INTERVAL '7 days'
```

## 8. Работа со Строками
```sql
-- Конкатенация
CONCAT(first_name, ' ', last_name)
-- Или оператор || (Postgres)
first_name || ' ' || last_name

-- Длина, регистр
LENGTH(text), LOWER(text), UPPER(text)

-- Обрезка пробелов
TRIM(text), LTRIM(text), RTRIM(text)

-- Подстрока
SUBSTRING(text FROM 1 FOR 5)

-- Замена
REPLACE(text, 'old', 'new')

-- Разбиение строки в массив (Postgres)
STRING_TO_ARRAY('a,b,c', ',') 
```

## 9. CTE (Common Table Expressions)
*Делает код читаемым. Аналог промежуточных DataFrame.*

```sql
WITH 
user_stats AS (
    SELECT user_id, AVG(amount) as avg_amt 
    FROM transactions 
    GROUP BY user_id
),
last_login AS (
    SELECT user_id, MAX(date) as last_date 
    FROM logins 
    GROUP BY user_id
)
SELECT 
    t1.*, 
    t2.avg_amt, 
    t3.last_date
FROM main_table t1
LEFT JOIN user_stats t2 ON t1.id = t2.user_id
LEFT JOIN last_login t3 ON t1.id = t3.user_id;
```

## 10. Postgres Специфика для DS
```sql
-- Работа с JSONB (если логи хранятся в JSON)
SELECT data->>'key' as value FROM logs; -- Извлечь значение как текст
SELECT data->'nested'->>'key' FROM logs; -- Вложенный JSON

-- Регулярные выражения (проверка на совпадение)
SELECT * FROM emails WHERE email ~* '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$';

-- Удаление дубликатов (оставить один)
DELETE FROM table a USING table b
WHERE a.id < b.id AND a.unique_key = b.unique_key;

-- Быстрая выборка случайных строк (для семплирования)
SELECT * FROM table TABLESAMPLE SYSTEM (10); -- ~10% данных
-- Или точно случайные (медленнее)
SELECT * FROM table ORDER BY RANDOM() LIMIT 1000;
```

## 11. Полезные паттерны для Feature Engineering

**1. Mean Encoding (Target Encoding через SQL):**
```sql
SELECT 
    t1.id,
    t1.category,
    t2.avg_target
FROM train t1
JOIN (
    SELECT category, AVG(target) as avg_target
    FROM train
    GROUP BY category
) t2 ON t1.category = t2.category;
```

**2. Days Since Last Event:**
```sql
SELECT 
    user_id,
    event_date,
    event_date - LAG(event_date) OVER (PARTITION BY user_id ORDER BY event_date) as days_since_prev
FROM events;
```

**3. Pivot (Поворот таблицы) через FILTER (Postgres 9.4+):**
*Вместо множества JOIN для подсчета категорий.*
```sql
SELECT 
    user_id,
    COUNT(*) FILTER (WHERE type = 'A') as count_type_A,
    COUNT(*) FILTER (WHERE type = 'B') as count_type_B,
    SUM(amount) FILTER (WHERE type = 'A') as sum_amount_A
FROM transactions
GROUP BY user_id;
```

---

### 💡 Совет на соревновании:
Если запрос становится слишком сложным — **разбивай его на CTE (WITH)**.
1.  Это легче отлаживать.
2.  Это легче читать через неделю.
3.  Оптимизатор PostgreSQL часто сам хорошо обрабатывает CTE (хотя иногда лучше использовать временные таблицы `CREATE TEMP TABLE` для огромных промежуточных данных).

Удачи! Если нужно что-то конкретное (например, работа с гео-данными PostGIS) — пиши.
