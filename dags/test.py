from datetime import datetime, timedelta
import sys
from pathlib import Path
import logging
import os
from dotenv import load_dotenv
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.models import Variable
from airflow.exceptions import AirflowException

# 載入環境變數
load_dotenv()

# 檢查必要的環境變數
required_env_vars = [
    'AIRFLOW_VAR_GCS_BUCKET_NAME',
    'AIRFLOW_VAR_GCS_PROJECT_ID',
    'AIRFLOW_VAR_GCS_FILE_NAME',
    'AIRFLOW_VAR_BQ_TABLE_ID'
]

missing_vars = [var for var in required_env_vars if not os.getenv(var)]
if missing_vars:
    raise AirflowException(f"缺少必要的環境變數: {', '.join(missing_vars)}")

# 改進日誌配置
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# 使用 try-except 包裝模組導入
try:
    from tasks.main_scrap import web_crawler, split_keywords
    from tasks.data_clean import clean_main
    from tasks.gcs_upload import upload_main
    from tasks.job_category import categorize_jobs
    from tasks.bigquery_load import load_to_bigquery
except ImportError as e:
    logger.error(f"模組導入失敗: {str(e)}")
    raise AirflowException(f"關鍵模組導入失敗: {str(e)}")

# 配置參數
CONFIG = {
    'SCRAPING_TIMEOUT': timedelta(hours=6),
    'PROCESSING_TIMEOUT': timedelta(hours=2),
    'RETRY_DELAY': timedelta(minutes=5),
    'MAX_RETRIES': 3
}

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': CONFIG['MAX_RETRIES'],
    'retry_delay': CONFIG['RETRY_DELAY'],
    'execution_timeout': CONFIG['PROCESSING_TIMEOUT']
}

def create_scraping_task(chunk, task_id):
    """創建單個爬蟲任務"""
    return PythonOperator(
        task_id=f'scrape_chunk_{task_id}',
        python_callable=web_crawler,
        op_kwargs={'key_texts_chunk': chunk},
        execution_timeout=CONFIG['SCRAPING_TIMEOUT'],
        retry_delay=CONFIG['RETRY_DELAY'],
        retries=CONFIG['MAX_RETRIES'],
        trigger_rule='all_success',
        pool='scraping_pool'
    )

def create_processing_task(task_id, python_callable, **kwargs):
    """創建處理任務的工廠函數"""
    return PythonOperator(
        task_id=task_id,
        python_callable=python_callable,
        execution_timeout=CONFIG['PROCESSING_TIMEOUT'],
        retries=CONFIG['MAX_RETRIES'],
        **kwargs
    )

# 創建 DAG
with DAG(
    'job_scraping_pipeline',
    default_args=default_args,
    description='104人力銀行職缺爬蟲 ETL 流程',
    schedule_interval='0 0 */7 * *',
    catchup=False,
    tags=['scraping', 'etl'],
    concurrency=6,
    max_active_runs=1,
    dagrun_timeout=timedelta(hours=8)
) as dag:
    
    # 爬蟲任務組
    with TaskGroup(group_id='scraping_tasks') as scraping_group:
        key_chunks = split_keywords()
        scraping_tasks = [
            create_scraping_task(chunk, i) 
            for i, chunk in enumerate(key_chunks)
        ]

    # 數據處理任務組
    with TaskGroup(group_id='data_processing') as processing_group:
        categorize_task = create_processing_task(
            'categorize_jobs',
            categorize_jobs,
            trigger_rule='all_success'
        )

        clean_task = create_processing_task(
            'clean_data',
            clean_main
        )

        upload_task = create_processing_task(
            'upload_to_gcs',
            upload_main
        )

        # 設定處理組內的任務依賴
        categorize_task >> clean_task >> upload_task

    # BigQuery 載入任務
    bq_load_task = create_processing_task(
        'load_to_bigquery',
        load_to_bigquery
    )

    # 設置任務組之間的依賴
    scraping_group >> processing_group >> bq_load_task

# 記錄環境信息
logger.info(f"Python 解釋器路徑: {sys.executable}")
logger.info(f"DAG ID: {dag.dag_id}")
logger.info(f"GCS Bucket: {os.getenv('AIRFLOW_VAR_GCS_BUCKET_NAME')}")
logger.info(f"BigQuery Table: {os.getenv('AIRFLOW_VAR_BQ_TABLE_ID')}")
