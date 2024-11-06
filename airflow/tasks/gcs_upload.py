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
        # 使用絕對路徑
        data_dir = Path('/opt/airflow/data')
        source_file = data_dir / '104data.cleaning.csv'
        
        # 確保目錄存在並有正確權限
        try:
            data_dir.mkdir(parents=True, exist_ok=True)
            os.chmod(data_dir, 0o777)
        except Exception as e:
            logging.error(f"設置目錄權限失敗: {e}")
            raise
            
        if not source_file.exists():
            raise FileNotFoundError(f"找不到源文件: {source_file}")
            
        uploader = GCSUploader()
        uploader.upload_file(str(source_file))
        
    except Exception as e:
        logging.error(f"GCS 上傳主程序執行錯誤: {e}")
        raise

if __name__ == "__main__":
    upload_main()