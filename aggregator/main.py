import os
import time
import asyncio
import logging
from datetime import datetime, timedelta, timezone
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException
from clickhouse_driver import Client
import psycopg2
from psycopg2.extras import execute_values

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

CH_HOST = os.getenv("CLICKHOUSE_HOST", "clickhouse")
CH_DB = os.getenv("CLICKHOUSE_DB", "movie_analytics")
PG_HOST = os.getenv("POSTGRES_HOST", "postgres")
PG_USER = os.getenv("POSTGRES_USER", "agg_user")
PG_PASS = os.getenv("POSTGRES_PASSWORD", "agg_password")
PG_DB = os.getenv("POSTGRES_DB", "analytics_db")
INTERVAL_SEC = int(os.getenv("AGGREGATION_INTERVAL_SEC", "3600"))

QUERIES = {
    "dau": "SELECT uniqExact(user_id) FROM movie_events WHERE toDate(timestamp) = %(date)s",
    "avg_view_time": "SELECT avgIf(progress_seconds, event_type = 'VIEW_FINISHED') FROM movie_events WHERE toDate(timestamp) = %(date)s",
    "conversion_rate": "SELECT round((countIf(event_type = 'VIEW_FINISHED') * 100.0) / nullIf(countIf(event_type = 'VIEW_STARTED'), 0), 2) FROM movie_events WHERE toDate(timestamp) = %(date)s",
    "retention_d1": """
        SELECT round((uniqExactIf(user_id, toDate(timestamp) = toDate(%(date)s) + 1) * 100.0) / nullIf(uniqExactIf(user_id, toDate(timestamp) = toDate(%(date)s)), 0), 2)
        FROM movie_events WHERE event_type = 'VIEW_STARTED' AND user_id IN (
            SELECT user_id FROM movie_events WHERE event_type = 'VIEW_STARTED' GROUP BY user_id HAVING min(toDate(timestamp)) = toDate(%(date)s)
        )
    """,
    "retention_d7": """
        SELECT round((uniqExactIf(user_id, toDate(timestamp) = toDate(%(date)s) + 7) * 100.0) / nullIf(uniqExactIf(user_id, toDate(timestamp) = toDate(%(date)s)), 0), 2)
        FROM movie_events WHERE event_type = 'VIEW_STARTED' AND user_id IN (
            SELECT user_id FROM movie_events WHERE event_type = 'VIEW_STARTED' GROUP BY user_id HAVING min(toDate(timestamp)) = toDate(%(date)s)
        )
    """
}

def get_ch_client():
    return Client(host=CH_HOST, port=9000, user="default", password="", database=CH_DB)

def get_pg_connection():
    return psycopg2.connect(host=PG_HOST, user=PG_USER, password=PG_PASS, dbname=PG_DB)

def calculate_and_save_metrics(target_date_str: str):
    logger.info(f"Начало вычисления метрик за: {target_date_str}")
    start_time = time.time()
    
    target_date = datetime.strptime(target_date_str, "%Y-%m-%d").date()
    
    ch = get_ch_client()
    metrics_to_save = []
    computed_at = datetime.now(timezone.utc)
    total_events = 0

    try:
        for metric_name, query in QUERIES.items():
            result = ch.execute(query, {"date": target_date})
            value = result[0][0] if result and result[0][0] is not None else 0.0
            
            if metric_name == "avg_view_time" and str(value) == 'nan':
                value = 0.0

            metrics_to_save.append((target_date, metric_name, "", float(value), computed_at))
            logger.info(f"Высчитана {metric_name}: {value}")

        top_result = ch.execute("""
            SELECT movie_id, count() as views FROM movie_events 
            WHERE toDate(timestamp) = %(date)s AND event_type = 'VIEW_STARTED' 
            GROUP BY movie_id ORDER BY views DESC LIMIT 10
        """, {"date": target_date})
        
        for rank, (movie_id, views) in enumerate(top_result, start=1):
            metrics_to_save.append((target_date, f"top_movie_rank_{rank}", movie_id, float(views), computed_at))

        total_events = ch.execute("SELECT count() FROM movie_events WHERE toDate(timestamp) = %(date)s", {"date": target_date})[0][0]

        if metrics_to_save:
            ch.execute("""
                INSERT INTO agg_daily_metrics (metric_date, metric_name, dimension, metric_value, computed_at)
                VALUES
            """, metrics_to_save)

        pg_conn = get_pg_connection()
        cursor = pg_conn.cursor()
        
        upsert_query = """
            INSERT INTO daily_metrics (metric_date, metric_name, dimension, metric_value, calculated_at)
            VALUES %s
            ON CONFLICT (metric_date, metric_name, dimension) 
            DO UPDATE SET 
                metric_value = EXCLUDED.metric_value,
                calculated_at = CURRENT_TIMESTAMP
        """
        execute_values(cursor, upsert_query, metrics_to_save)
        pg_conn.commit()
        cursor.close()
        pg_conn.close()

        elapsed_time = time.time() - start_time
        logger.info(f"Завершено за {elapsed_time:.2f} сек. Обработано событий: {total_events}. Сохранено метрик: {len(metrics_to_save)}.")
        
    except Exception as e:
        logger.error(f"Ошибка: {e}")
        raise
    finally:
        ch.disconnect()


background_task = None

@asynccontextmanager
async def lifespan(app: FastAPI):
    global background_task
    
    async def scheduler():
        while True:
            yesterday = (datetime.now(timezone.utc) - timedelta(days=1)).strftime("%Y-%m-%d")
            logger.info(f"Запланированный запуск за {yesterday}")
            try:
                await asyncio.to_thread(calculate_and_save_metrics, yesterday)
            except Exception as e:
                logger.error(f"Ошибка запланированного запуска {e}")
            await asyncio.sleep(INTERVAL_SEC)
            
    background_task = asyncio.create_task(scheduler())
    logger.info("Сервис агрегации запущен")
    yield
    logger.info("Сервис агрегации завершил работу.")
    if background_task:
        background_task.cancel()

app = FastAPI(title="Aggregation Service", lifespan=lifespan)

@app.post("/aggregate")
async def manual_aggregate(date: str):
    try:
        datetime.strptime(date, "%Y-%m-%d")
        await asyncio.to_thread(calculate_and_save_metrics, date)
        return {"status": "success", "date": date}
    except ValueError:
        raise HTTPException(status_code=400, detail="Некорректный формат даты (верный YYYY-MM-DD)")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))