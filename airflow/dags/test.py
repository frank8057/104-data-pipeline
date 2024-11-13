#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from datetime import datetime, timedelta
import sys
from pathlib import Path
import logging
import os
from dotenv import load_dotenv
import json
import time
import sys
sys.path.append('/opt/airflow')

# 添加父目錄到 Python 路徑
current_dir = Path(__file__).resolve().parent
parent_dir = current_dir.parent
sys.path.append(str(parent_dir))

# Airflow 相關導入
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.models import Variable
from airflow.exceptions import AirflowException
from airflow.models.baseoperator import cross_downstream, chain
from airflow.operators.empty import EmptyOperator

# 載入環境變數
# load_dotenv()

# 檢查必要的 Airflow 變數
required_variables = [
    'GCS_BUCKET_NAME',
    'GCS_PROJECT_ID',
    'GCS_FILE_NAME',
    'BQ_TABLE_ID'
]

# 環境變數驗證函數
def validate_env_vars():
    missing_vars = []
    for var_name in required_variables:
        try:
            value = Variable.get(var_name)
            logger.info(f"{var_name}: {value}")
        except KeyError:
            missing_vars.append(var_name)
    
    if missing_vars:
        error_msg = f"缺少必要的 Airflow 變數: {', '.join(missing_vars)}"
        logger.error(error_msg)
        raise AirflowException(error_msg)

# 改進日誌配置
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# 驗證環境變數
validate_env_vars()

# 使用 try-except 包裝模組導入
try:
    from tasks.main_scrap import web_crawler, split_keywords
    from tasks.data_clean import clean_main
    from tasks.gcs_upload import upload_main
    from tasks.job_category import categorize_jobs
    from tasks.bigquery_load import load_to_bigquery
except ImportError as e:
    logger.error(f"模組導入失敗: {str(e)}")
    logger.error(f"當前 Python 路徑: {sys.path}")
    logger.error(f"當前工作目錄: {os.getcwd()}")
    raise AirflowException(f"關鍵模組導入失敗: {str(e)}")

# 配置參數
CONFIG = {
    'REQUEST_DELAY': 3,  # 每次請求延遲3秒
    'CHUNK_SIZE': 100,  # 每個任務處理100個職位
    'SCRAPING_TIMEOUT': timedelta(hours=48),  # 單個爬蟲任務超時
    'PROCESSING_TIMEOUT': timedelta(hours=4),
    'RETRY_DELAY': timedelta(minutes=5),
    'MAX_RETRIES': 3,
    'MAX_ACTIVE_RUNS': 1
}

# 預設參數
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': CONFIG['MAX_RETRIES'],
    'retry_delay': CONFIG['RETRY_DELAY'],
    'retry_exponential_backoff': True,
    'max_retry_delay': timedelta(minutes=30),
    'execution_timeout': CONFIG['PROCESSING_TIMEOUT']
}

def create_processing_task(task_id, python_callable, **kwargs):
    """創建處理任務的輔助函數"""
    return PythonOperator(
        task_id=task_id,
        python_callable=python_callable,
        execution_timeout=CONFIG['PROCESSING_TIMEOUT'],
        retries=CONFIG['MAX_RETRIES'],
        **kwargs
    )

# 檢查是否在 worker 中運行
current_dag = None
if len(sys.argv) > 3:
    current_dag = sys.argv[3]
    logger.info(f"Running in worker mode for DAG: {current_dag}")

# 創建 DAG
with DAG(
    'job_scraping_pipeline',
    default_args=default_args,
    description='104人力銀行職缺爬蟲 ETL 流程',
    schedule_interval='0 0 */7 * *',
    catchup=False,
    tags=['scraping', 'etl'],
    dagrun_timeout=timedelta(hours=72),
    max_active_runs=CONFIG['MAX_ACTIVE_RUNS'],
    concurrency=16,
    render_template_as_native_obj=True,
    default_view='graph'
) as dag:
    
    # 爬蟲任務組
    with TaskGroup(group_id='scraping_tasks') as scraping_group:
        # 使用 split_keywords 函數獲取分組後的關鍵字
        try:
            key_chunks = split_keywords()
            logger.info(f"成功獲取關鍵字分組,共 {len(key_chunks)} 組")
        except Exception as e:
            logger.error(f"獲取關鍵字分組失敗: {str(e)}")
            raise AirflowException(f"關鍵字分組失敗: {str(e)}")
        
        # 創建並行的爬蟲任務
        scraping_tasks = []
        for i, chunk in enumerate(key_chunks):
            task_id = f'scrape_chunk_{i}'
            if current_dag is not None and f"scraping_tasks.{task_id}" not in current_dag:
                logger.debug(f"跳過任務 {task_id} (worker 模式)")
                continue
                
            task = PythonOperator(
                task_id=task_id,
                python_callable=web_crawler,
                op_kwargs={
                    'key_texts_chunk': chunk,
                    'chunk_index': i
                },
                execution_timeout=CONFIG['SCRAPING_TIMEOUT'],
                retries=CONFIG['MAX_RETRIES'],
                retry_delay=CONFIG['RETRY_DELAY'],
                pool='scraping_pool',  # 添加資源池控制
                pool_slots=1
            )
            scraping_tasks.append(task)
            logger.info(f"創建爬蟲任務 {task_id}, 處理 {len(chunk)} 個關鍵字")

        if not scraping_tasks:
            logger.warning("沒有創建任何爬蟲任務!")
        
        # 新增 finish_scraping 作為匯聚點
        finish_scraping = EmptyOperator(
            task_id='finish_scraping',
            trigger_rule='all_success'
        )
        
        # 設置依賴關係
        scraping_tasks >> finish_scraping

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
        
        categorize_task >> clean_task >> upload_task

    # BigQuery 載入任務
    bq_load_task = create_processing_task(
        'load_to_bigquery',
        load_to_bigquery
    )

    # 設置任務組之間的依賴
    finish_scraping >> processing_group >> bq_load_task

# 記錄環境信息
logger.info(f"Python 解釋器路徑: {sys.executable}")
logger.info(f"Python 版本: {sys.version}")
logger.info(f"DAG ID: {dag.dag_id}")
