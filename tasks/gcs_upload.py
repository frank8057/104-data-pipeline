from datetime import datetime
import logging
from pathlib import Path
from google.cloud import storage
from google.api_core import retry
import os
from dotenv import load_dotenv

# 載入 .env 檔案
load_dotenv()

class GCSUploader:
    def __init__(self, bucket_name=None, project_id=None):
        # 使用環境變數,如果沒有傳入參數的話
        self.bucket_name = bucket_name or os.getenv('GCS_BUCKET_NAME')
        self.project_id = project_id or os.getenv('GCS_PROJECT_ID')
        self.setup_logging()
        
    def setup_logging(self):
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger(__name__)
    
    @retry.Retry(predicate=retry.if_exception_type(Exception))
    def upload_file(self, source_file_path, destination_blob_name=None):
        try:
            client = storage.Client(project=self.project_id)
            bucket = client.bucket(self.bucket_name)
            
            if destination_blob_name is None:
                destination_blob_name = Path(source_file_path).name
            
            blob = bucket.blob(destination_blob_name)
            blob.upload_from_filename(source_file_path)
            
            self.logger.info(
                f"文件 {source_file_path} 已成功上傳至 gs://{self.bucket_name}/{destination_blob_name}"
            )
            return True
            
        except Exception as e:
            self.logger.error(f"上傳文件到 GCS 時發生錯誤: {str(e)}")
            raise

def upload_main():
    try:
        # 使用相對於當前專案的路徑
        current_dir = Path(__file__).parent.parent  # 獲取 airflow 目錄
        data_dir = current_dir / 'data'  # 指向 airflow/data 目錄
        
        # 設定源文件路徑
        source_file = str(data_dir / '104data.cleaning.csv')
        
        # 添加調試日誌
        logging.info(f"當前工作目錄: {Path.cwd()}")
        logging.info(f"數據目錄路徑: {data_dir}")
        logging.info(f"源文件路徑: {source_file}")
        logging.info(f"目錄是否存在: {data_dir.exists()}")
        
        # 驗證文件是否存在
        if not Path(source_file).exists():
            logging.error(f"目錄內容: {list(data_dir.glob('*'))}")
            raise FileNotFoundError(f"找不到源文件: {source_file}")
            
        # 初始化上傳器 - 不需要硬編碼參數
        uploader = GCSUploader()
        
        # 執行上傳
        uploader.upload_file(source_file)
        
    except Exception as e:
        logging.error(f"GCS 上傳主程序執行錯誤: {e}")
        raise

if __name__ == "__main__":
    upload_main()