#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from datetime import datetime, timedelta
import logging
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.models import Variable
from airflow.exceptions import AirflowException
from airflow.operators.empty import EmptyOperator

# 設置日誌
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# **檢查環境變數**
REQUIRED_VARS = ["GCS_BUCKET_NAME", "GCS_PROJECT_ID", "GCS_FILE_NAME", "BQ_TABLE_ID"]

def validate_env_vars():
    missing_vars = [var for var in REQUIRED_VARS if not Variable.get(var, default_var=None)]
    if missing_vars:
        raise AirflowException(f"缺少必要的 Airflow 變數: {', '.join(missing_vars)}")
    logger.info(f"所有必要的 Airflow 變數已成功載入")

# **導入 Tasks**
try:
    from tasks.main_scrap import web_crawler, split_keywords
    from tasks.data_clean import clean_main
    from tasks.gcs_upload import upload_main
    from tasks.job_category import categorize_jobs
    from tasks.bigquery_load import load_to_bigquery
except ImportError as e:
    raise AirflowException(f"模組導入失敗: {e}")

# **DAG 配置**
default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 1, 1),
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "job_scraping_pipeline",
    default_args=default_args,
    schedule_interval="0 0 */7 * *",
    catchup=False,
    tags=["scraping", "etl"]
) as dag:

    validate_env_task = PythonOperator(
        task_id="validate_env_vars",
        python_callable=validate_env_vars,
        retries=0
    )

    with TaskGroup("scraping_tasks") as scraping_group:
        key_chunks = split_keywords()
        scraping_tasks = [PythonOperator(
            task_id=f"scrape_chunk_{i}",
            python_callable=web_crawler,
            op_kwargs={"key_texts_chunk": chunk, "chunk_index": i}
        ) for i, chunk in enumerate(key_chunks)]
        finish_scraping = EmptyOperator(task_id="finish_scraping")
        scraping_tasks >> finish_scraping

    categorize_task = PythonOperator(task_id="categorize_jobs", python_callable=categorize_jobs)
    clean_task = PythonOperator(task_id="clean_data", python_callable=clean_main)
    upload_task = PythonOperator(task_id="upload_to_gcs", python_callable=upload_main)
    bq_load_task = PythonOperator(task_id="load_to_bigquery", python_callable=load_to_bigquery)

    validate_env_task >> scraping_group >> [categorize_task, clean_task] >> upload_task >> bq_load_task
