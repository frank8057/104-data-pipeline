from google.cloud import bigquery
import logging
from datetime import datetime
import os
from dotenv import load_dotenv

# 載入環境變數
load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def load_to_bigquery():
    """將 GCS 的數據載入到 BigQuery (TRUNCATE 模式)"""
    try:
        # 初始化 BigQuery 客戶端
        client = bigquery.Client()

        # 設置任務配置
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.CSV,
            skip_leading_rows=1,  # 跳過 CSV 標題行
            autodetect=True,      # 自動檢測 schema
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE  # 覆蓋現有數據
            # write_disposition=bigquery.WriteDisposition.WRITE_APPEND,  # 追加新數據
            # # 添加時間戳記欄位
            # schema_update_options=[
            #     bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION
            # ]
        )

        # 從環境變數構建 GCS URI
        bucket_name = os.getenv('GCS_BUCKET_NAME')
        file_name = os.getenv('GCS_FILE_NAME')
        gcs_uri = f"gs://{bucket_name}/{file_name}"
        
        # 從環境變數獲取 table_id
        table_id = os.getenv('BQ_TABLE_ID')

        # 記錄載入開始時間
        start_time = datetime.now()
        
        # 開始載入任務
        load_job = client.load_table_from_uri(
            gcs_uri,
            table_id,
            job_config=job_config
        )

        # 等待任務完成
        load_job.result()
        
        # 計算並記錄執行時間
        elapsed_time = (datetime.now() - start_time).total_seconds()
        logger.info(f"載入完成，執行時間: {elapsed_time} 秒")

        # 獲取結果
        destination_table = client.get_table(table_id)
        
        # # 計算新增的行數 (APPEND 模式使用)
        # query = f"""
        # SELECT COUNT(*) as new_rows
        # FROM `{table_id}`
        # WHERE DATE(_PARTITIONTIME) = DATE('{start_time.date()}')
        # """
        
        # query_job = client.query(query)
        # results = query_job.result()
        
        # for row in results:
        #     new_rows = row.new_rows
        #     logger.info(f"已追加 {new_rows} 行到 {table_id}")

        # 記錄總行數
        logger.info(f"表格總行數: {destination_table.num_rows}")

    except Exception as e:
        logger.error(f"BigQuery 載入失敗: {str(e)}")
        raise

def get_table_schema():
    """定義表格 schema"""
    return [
        bigquery.SchemaField("report_date", "DATE"),
        bigquery.SchemaField("job_title", "STRING"),
        bigquery.SchemaField("company_name", "STRING"),
        bigquery.SchemaField("main_category", "STRING"),
        bigquery.SchemaField("sub_category", "STRING"),
        bigquery.SchemaField("job_category", "STRING"),
        bigquery.SchemaField("salary", "INTEGER"),
        bigquery.SchemaField("location_region", "STRING"),
        bigquery.SchemaField("experience", "STRING"),
        bigquery.SchemaField("industry", "STRING"),
        bigquery.SchemaField("job_url", "STRING"),
        bigquery.SchemaField("job_skills", "STRING"),
        bigquery.SchemaField("tools", "STRING"),
        bigquery.SchemaField("source", "STRING"),
        # bigquery.SchemaField("scrape_date", "DATE"),
        bigquery.SchemaField("insert_timestamp", "TIMESTAMP")  # 保留此欄位用於分區
    ]

def create_table_if_not_exists():
    """如果表格不存在則創建"""
    try:
        client = bigquery.Client()
        # 從環境變數獲取 table_id
        table_id = os.getenv('BQ_TABLE_ID')
        
        if not table_id:
            raise ValueError("未設置 BQ_TABLE_ID 環境變數")
            
        schema = get_table_schema()
        
        table = bigquery.Table(table_id, schema=schema)
        table.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="insert_timestamp"  # 使用此欄位進行分區
        )
        
        table = client.create_table(table, exists_ok=True)
        logger.info(f"表格 {table_id} 已準備就緒")
        
    except Exception as e:
        logger.error(f"創建表格失敗: {str(e)}")
        raise