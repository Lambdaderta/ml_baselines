Отлично! Apache Airflow — это стандарт оркестрации пайплайнов. В контексте соревнований (особенно хакатонов с треком MLOps или сложных инфраструктурных задач) умение быстро поднять ETL — огромный плюс.

Вот шпаргалка по **Apache Airflow 2.x** с полным примером ETL на базе **Iris** и **PostgreSQL**.

---

# ✈️ Apache Airflow Шпаргалка (для ML/ETL)

## 1. Быстрый старт (Docker)
Самый надежный способ поднять Airflow локально — через `docker-compose`.
```bash
# Скачиваем официальный compose файл
curl -LfO 'https://airflow.apache.org/docs/apache-airflow/stable/docker-compose.yaml'
mkdir -p ./dags ./logs ./plugins
echo -e "AIRFLOW_UID=$(id -u)" > .env

# Запуск
docker compose up airflow-init
docker compose up
```
*Веб-интерфейс:* `http://localhost:8080` (логин/пароль: `airflow`/`airflow`)

## 2. Основные понятия
*   **DAG (Directed Acyclic Graph):** Скрипт пайплайна.
*   **Task:** Отдельный шаг (например, "скачать файл", "вставить в БД").
*   **Operator:** Шаблон задачи (PythonOperator, PostgresOperator).
*   **Hook:** Интерфейс для подключения к внешней системе (БД, S3).
*   **Connection:** Настройки доступа (логин, пароль, хост), хранятся в UI Airflow.
*   **XCom:** Механизм обмена маленькими данными между задачами (не для DataFrames!).

## 3. Настройка подключения к PostgreSQL
1.  Зайди в Airflow UI -> **Admin** -> **Connections**.
2.  Нажми **+**.
3.  **Conn Id:** `postgres_default` (или свое имя, например `my_pg`).
4.  **Conn Type:** `Postgres`.
5.  **Host:** `host.docker.internal` (если БД на твоем ПК) или IP контейнера.
6.  **Login/Password/Port/DB:** Твои данные.
7.  *Важно:* Если Airflow в докере, а Postgres на хосте, используй `host.docker.internal` (для Mac/Win) или IP шлюза (Linux).

---

## 4. Полный пример ETL: Iris Dataset
**Задача:**
1.  Скачать CSV с Iris.
2.  Загрузить в PostgreSQL.
3.  Добавить признак (площадь чашелистика) через SQL.
4.  Выгрузить готовый датасет обратно в CSV для ML.

**Файл:** `dags/iris_etl_pipeline.py`

```python
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import pandas as pd
import requests
import os

# --- Конфигурация ---
DEFAULT_ARGS = {
    'owner': 'ml_competitor',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# ID подключения к БД (настроенного в UI)
PG_CONN_ID = 'postgres_default'
TABLE_NAME = 'iris_raw'
TABLE_PROCESSED = 'iris_processed'

# --- Функции (Python Tasks) ---

def download_iris_data(**context):
    """Скачивает датасет и сохраняет во временную папку Airflow"""
    url = "https://archive.ics.uci.edu/ml/machine-learning-databases/iris/iris.data"
    # Путь внутри контейнера Airflow
    file_path = '/opt/airflow/data/iris_raw.csv' 
    os.makedirs('/opt/airflow/data', exist_ok=True)
    
    response = requests.get(url)
    with open(file_path, 'wb') as f:
        f.write(response.content)
    
    # Передаем путь следующей задаче через XCom
    return file_path

def load_to_postgres(**context):
    """Загружает CSV в Postgres через Pandas (быстрее чем COPY для малых данных)"""
    ti = context['ti']
    file_path = ti.xcom_pull(task_ids='download_data')
    
    # Читаем CSV (у Iris нет заголовка в источнике)
    cols = ['sepal_length', 'sepal_width', 'petal_length', 'petal_width', 'class']
    df = pd.read_csv(file_path, names=cols)
    
    # Подключаемся к БД
    hook = PostgresHook(postgres_conn_id=PG_CONN_ID)
    engine = hook.get_sqlalchemy_engine()
    
    # Записываем (if_exists='replace' для идемпотентности)
    df.to_sql(TABLE_NAME, engine, if_exists='replace', index=False)
    print(f"Loaded {len(df)} rows to {TABLE_NAME}")

def export_processed_data(**context):
    """Выгружает обработанные данные для ML"""
    hook = PostgresHook(postgres_conn_id=PG_CONN_ID)
    engine = hook.get_sqlalchemy_engine()
    
    query = f"SELECT * FROM {TABLE_PROCESSED}"
    df = pd.read_sql(query, engine)
    
    output_path = '/opt/airflow/data/iris_final.csv'
    df.to_csv(output_path, index=False)
    print(f"Exported data to {output_path}")

# --- Определение DAG ---

with DAG(
    'iris_etl_pipeline',
    default_args=DEFAULT_ARGS,
    description='ETL для Iris с Postgres',
    schedule_interval=None, # Запуск вручную (On Demand)
    catchup=False,
    tags=['ml', 'competition', 'postgres'],
) as dag:

    # 1. Скачивание
    task_download = PythonOperator(
        task_id='download_data',
        python_callable=download_iris_data,
    )

    # 2. Создание таблицы (SQL)
    # DROP IF EXISTS обеспечивает идемпотентность (повторный запуск не сломает пайплайн)
    task_create_table = PostgresOperator(
        task_id='create_table',
        postgres_conn_id=PG_CONN_ID,
        sql=f"""
            DROP TABLE IF EXISTS {TABLE_NAME};
            CREATE TABLE {TABLE_NAME} (
                sepal_length FLOAT,
                sepal_width FLOAT,
                petal_length FLOAT,
                petal_width FLOAT,
                class TEXT
            );
        """,
    )

    # 3. Загрузка данных (Python + Pandas)
    task_load = PythonOperator(
        task_id='load_data_to_pg',
        python_callable=load_to_postgres,
    )

    # 4. Трансформация (Feature Engineering в SQL)
    task_transform = PostgresOperator(
        task_id='feature_engineering',
        postgres_conn_id=PG_CONN_ID,
        sql=f"""
            DROP TABLE IF EXISTS {TABLE_PROCESSED};
            CREATE TABLE {TABLE_PROCESSED} AS
            SELECT 
                *,
                (sepal_length * sepal_width) as sepal_area,
                (petal_length * petal_width) as petal_area,
                NOW() as created_at
            FROM {TABLE_NAME};
        """,
    )

    # 5. Экспорт результата
    task_export = PythonOperator(
        task_id='export_for_ml',
        python_callable=export_processed_data,
    )

    # --- Зависимости (Оркестрация) ---
    task_download >> task_create_table >> task_load >> task_transform >> task_export
```

---

## 5. Шпаргалка по Операторам и Хукам

### PythonOperator
Используй для логики на Python (парсинг, вызов API, сложная предобработка).
```python
PythonOperator(
    task_id='my_task',
    python_callable=my_function,
    op_kwargs={'arg1': 'value'}, # Аргументы функции
    provide_context=True, # Доступ к ti (task instance)
)
```
*Доступ к XCom:* `ti.xcom_pull(task_ids='prev_task')`
*Отправка в XCom:* `return value` из функции (автоматически) или `ti.xcom_push(key='key', value=val)`

### PostgresOperator
Используй для DDL/DML запросов (CREATE, INSERT, UPDATE, агрегации).
```python
PostgresOperator(
    task_id='sql_task',
    postgres_conn_id='my_conn',
    sql="""
        SELECT * FROM table WHERE date = '{{ ds }}'; 
        -- {{ ds }} это макрос даты выполнения (YYYY-MM-DD)
    """,
)
```

### PostgresHook
Используй в PythonOperator для гибкой работы с БД (например, pandas `to_sql`).
```python
hook = PostgresHook(postgres_conn_id='my_conn')
conn = hook.get_conn() # Получает psycopg2 connection
cursor = conn.cursor()
cursor.execute("SELECT 1")

# Или SQLAlchemy engine для pandas
engine = hook.get_sqlalchemy_engine()
```

---

## 6. Важные макросы (Jinja Templating)
В SQL внутри Airflow можно использовать переменные:
*   `{{ ds }}` — Дата выполнения (2023-10-25)
*   `{{ ds_nodash }}` — Дата без тире (20231025)
*   `{{ execution_date }}` — Полное время
*   `{{ params.my_param }}` — Пользовательские параметры
*   `{{ ti.xcom_pull(task_ids='task_id') }}` — Получить данные из другой задачи прямо в SQL (редко, но можно).

---

## 7. Советы для соревнований (ML Ops)

1.  **Идемпотентность:** Пайплайн должен запускаться 10 раз подряд без ошибок.
    *   Используй `DROP TABLE IF EXISTS` перед `CREATE`.
    *   Или `INSERT ... ON CONFLICT DO NOTHING`.
    *   В `to_sql` используй `if_exists='replace'` (для тестов) или `'append'` (для прода).
2.  **XCom Лимит:** По умолчанию XCom хранит до 48KB данных. **Не передавай DataFrames через XCom!** Передавай только пути к файлам или имена таблиц.
3.  **Variables:** Храни пути и конфиги в **Admin -> Variables**, а не в коде.
    *   В коде: `from airflow.models import Variable; path = Variable.get("data_path")`
4.  **Backfill:** Если нужно прогнать пайплайн за прошлые даты:
    ```bash
    airflow dags backfill -s 2023-01-01 -e 2023-01-31 iris_etl_pipeline
    ```
5.  **Логи:** Если задача упала, смотри логи в UI (кнопка "Log" в квадрате задачи). Там будет traceback Python.
6.  **Зависимости пакетов:** Если нужен `pandas` или `requests` внутри задач Airflow, их нужно установить в образ Airflow.
    *   В Docker: создай `requirements.txt` и добавь в `docker-compose.yaml` volume или собери свой образ.
    *   Быстро (для локальных тестов): `docker exec -it <airflow_worker_container> pip install pandas requests`

## 8. Чек-лист перед запуском
1.  [ ] Поднятие Airflow (docker compose up).
2.  [ ] Создание Connection к Postgres в UI.
3.  [ ] Копирование файла DAG в папку `./dags`.
4.  [ ] Проверка появления DAG в UI (может занять 10-30 сек).
5.  [ ] Запуск (Play button).
6.  [ ] Проверка логов каждой задачи.
7.  [ ] Проверка данных в Postgres (`SELECT * FROM iris_processed`).

Удачи! Этот пайплайн покажет жюри, что ты умеешь не просто писать `fit/predict`, но и строить надежную инфраструктуру данных. 🚀

---

Да, **`airflow standalone`** — это отличный вариант для локальной разработки и соревнований. Он запускает веб-сервер, планировщик (scheduler) и воркер в одном процессе. Это проще, чем Docker, не требует настройки `docker-compose`, но работает только на одной машине (что для соло-соревнования идеально).

Вот адаптированная шпаргалка под **Airflow Standalone** (без Docker).

---

# ✈️ Apache Airflow Standalone Шпаргалка

## 1. Быстрая установка (Local)
*Требует Python 3.8+*

```bash
# 1. Создаем виртуальное окружение (рекомендуется)
python -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate

# 2. Установка Airflow + зависимости для Postgres и ML
# Версия 2.8+ рекомендуется
pip install "apache-airflow==2.8.0" --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.8.0/constraints-3.8.txt"
pip install apache-airflow-providers-postgres pandas sqlalchemy psycopg2-binary requests

# 3. Инициализация и запуск (одной командой!)
# Это создаст БД (SQLite по умолчанию), пользователя и запустит сервер
airflow standalone
```
*После запуска в логах будет логин/пароль (обычно `admin` / случайный пароль).*
*Веб-интерфейс:* `http://localhost:8080`

---

## 2. Настройка подключения к PostgreSQL
В Standalone режиме твой компьютер — это и есть сервер Airflow. Поэтому хост БД — `localhost`.

**Вариант А: Через CLI (быстро для скриптов)**
```bash
airflow connections add 'postgres_default' \
    --conn-type 'postgres' \
    --conn-host 'localhost' \
    --conn-login 'postgres' \
    --conn-password 'your_password' \
    --conn-port '5432' \
    --conn-schema 'mydb'
```

**Вариант Б: Через UI**
1.  Зайди на `http://localhost:8080`.
2.  Admin -> Connections -> Create.
3.  Conn Id: `postgres_default`.
4.  Conn Type: `Postgres`.
5.  Host: `localhost` (не `host.docker.internal`!).
6.  Остальное как обычно.

---

## 3. Структура папок
В Standalone нет томов Docker. Все пути — локальные.
```bash
$AIRFLOW_HOME/
├── dags/            # Сюда кладем файлы .py
├── logs/            # Логи задач
├── plugins/         # Плагины
├── data/            # Твои данные (создай вручную)
└── airflow.db       # SQLite БД самого Airflow
```
*Узнать путь `$AIRFLOW_HOME` можно командой `echo $AIRFLOW_HOME`.*

---

## 4. ETL Пайплайн (Адаптированный под Standalone)
**Файл:** `dags/iris_etl_standalone.py`

```python
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import pandas as pd
import requests
import os

# --- Конфигурация ---
DEFAULT_ARGS = {
    'owner': 'ml_competitor',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
}

PG_CONN_ID = 'postgres_default'
TABLE_NAME = 'iris_raw'
TABLE_PROCESSED = 'iris_processed'

# Путь к папке данных (относительно AIRFLOW_HOME или абсолютный)
# В standalone лучше использовать абсолютный путь или os.getcwd()
DATA_DIR = os.path.join(os.environ.get('AIRFLOW_HOME'), 'data')
os.makedirs(DATA_DIR, exist_ok=True)

# --- Функции ---

def download_iris_data(**context):
    url = "https://archive.ics.uci.edu/ml/machine-learning-databases/iris/iris.data"
    file_path = os.path.join(DATA_DIR, 'iris_raw.csv')
    
    response = requests.get(url)
    with open(file_path, 'wb') as f:
        f.write(response.content)
    
    return file_path

def load_to_postgres(**context):
    ti = context['ti']
    file_path = ti.xcom_pull(task_ids='download_data')
    
    cols = ['sepal_length', 'sepal_width', 'petal_length', 'petal_width', 'class']
    df = pd.read_csv(file_path, names=cols)
    
    hook = PostgresHook(postgres_conn_id=PG_CONN_ID)
    engine = hook.get_sqlalchemy_engine()
    
    # replace удалит таблицу и создаст заново (идемпотентность)
    df.to_sql(TABLE_NAME, engine, if_exists='replace', index=False)
    print(f"Loaded {len(df)} rows")

def export_processed_data(**context):
    hook = PostgresHook(postgres_conn_id=PG_CONN_ID)
    engine = hook.get_sqlalchemy_engine()
    
    query = f"SELECT * FROM {TABLE_PROCESSED}"
    df = pd.read_sql(query, engine)
    
    output_path = os.path.join(DATA_DIR, 'iris_final.csv')
    df.to_csv(output_path, index=False)
    print(f"Exported to {output_path}")

# --- DAG ---

with DAG(
    'iris_etl_standalone',
    default_args=DEFAULT_ARGS,
    description='ETL Iris Standalone',
    schedule_interval=None,
    catchup=False,
    tags=['ml', 'standalone'],
) as dag:

    task_download = PythonOperator(
        task_id='download_data',
        python_callable=download_iris_data,
    )

    task_create_table = PostgresOperator(
        task_id='create_table',
        postgres_conn_id=PG_CONN_ID,
        sql=f"""
            DROP TABLE IF EXISTS {TABLE_NAME};
            CREATE TABLE {TABLE_NAME} (
                sepal_length FLOAT,
                sepal_width FLOAT,
                petal_length FLOAT,
                petal_width FLOAT,
                class TEXT
            );
        """,
    )

    task_load = PythonOperator(
        task_id='load_data_to_pg',
        python_callable=load_to_postgres,
    )

    task_transform = PostgresOperator(
        task_id='feature_engineering',
        postgres_conn_id=PG_CONN_ID,
        sql=f"""
            DROP TABLE IF EXISTS {TABLE_PROCESSED};
            CREATE TABLE {TABLE_PROCESSED} AS
            SELECT 
                *,
                (sepal_length * sepal_width) as sepal_area,
                (petal_length * petal_width) as petal_area
            FROM {TABLE_NAME};
        """,
    )

    task_export = PythonOperator(
        task_id='export_for_ml',
        python_callable=export_processed_data,
    )

    task_download >> task_create_table >> task_load >> task_transform >> task_export
```

---

## 5. Важные отличия Standalone от Docker

| Фича | Docker Compose | Standalone |
| :--- | :--- | :--- |
| **Запуск** | `docker compose up` | `airflow standalone` |
| **БД Airflow** | Обычно Postgres | **SQLite** (по умолчанию) |
| **Хост БД** | `host.docker.internal` | `localhost` |
| **Пути** | `/opt/airflow/...` | Локальные пути (`$AIRFLOW_HOME`) |
| **Пользователь** | Создается при инициализации | **Авто-создание** (пароль в логе) |
| **Ресурсы** | Изолированы контейнерами | Едят ресурсы твоего ПК напрямую |
| **Надежность** | Высокая (продакшен готов) | Низкая (только для дев/теста) |

---

## 6. Частые проблемы и решения

**1. Ошибка "No module named airflow.providers..."**
*   **Решение:** В Airflow 2.x провайдеры вынесены в отдельные пакеты.
    ```bash
    pip install apache-airflow-providers-postgres
    ```

**2. Ошибка подключения к Postgres (Connection refused)**
*   **Причина:** В Docker мы использовали спец. хост, тут нужен обычный.
*   **Решение:** Убедись, что в Connection в Airflow указан `Host: localhost`, а не `host.docker.internal`. Убедись, что твой Postgres слушает localhost (в `pg_hba.conf`).

**3. DAG не появляется в UI**
*   **Решение:** 
    1. Проверь синтаксис: `python dags/iris_etl_standalone.py` (не должно быть ошибок).
    2. Проверь папку: `airflow config get-value core dags_folder`. Файл должен лежать там.
    3. В Standalone иногда нужно подождать 30-60 сек, пока планировщик просканирует папку.

**4. Блокировка SQLite (Database is locked)**
*   **Причина:** SQLite не любит параллельную запись.
*   **Решение:** Для соревнований это редкость. Если случится — уменьши `parallelism` в `airflow.cfg` или переключи метабазу Airflow на Postgres (см. ниже).

---

## 7. Продвинутый уровень: Airflow MetaDB на Postgres
Если хочешь, чтобы сам Airflow хранил свои данные в Postgres (надежнее SQLite), задай переменную окружения **перед** запуском `airflow standalone`:

**Linux/Mac:**
```bash
export AIRFLOW__DATABASE__SQL_ALCHEMY_CONN="postgresql+psycopg2://user:pass@localhost:5432/airflow_db"
airflow standalone
```

**Windows (PowerShell):**
```powershell
$env:AIRFLOW__DATABASE__SQL_ALCHEMY_CONN="postgresql+psycopg2://user:pass@localhost:5432/airflow_db"
airflow standalone
```
*В этом случае базу `airflow_db` нужно создать вручную в Postgres заранее.*

---

## 8. Итог: Что выбрать для соревнования?

1.  **Если есть Docker и время:** Используй **Docker Compose**. Это ближе к продакшену, меньше проблем с зависимостями Python на хосте.
2.  **Если нужно быстро / нет Docker / мало RAM:** Используй **Standalone**. 
    *   Ставится за 2 минуты.
    *   Легче дебажить (все логи в одном терминале + файлы).
    *   Проще пути к файлам (не нужно мапить тома).

Для ML соревнования **Standalone** часто удобнее, так как ты можешь легко импортировать свои локальные библиотеки и скрипты в DAG без сборки образов.

Удачи! 🚀

