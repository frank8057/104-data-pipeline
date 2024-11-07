import time
from datetime import date, datetime
import csv
from urllib.parse import quote
from tqdm import tqdm
import logging
import random
import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util import Retry
from pathlib import Path
import re
import signal
from contextlib import contextmanager
import os
from airflow.exceptions import AirflowException

# 配置日誌
logging.basicConfig(level=logging.INFO)

def split_keywords(max_keywords_per_chunk=5):
    """將關鍵字列表分成更小的chunks"""
    try:
        key_texts = [
            "軟體工程師", "Software Developer", "通訊軟體工程師",
            "韌體工程師", "Firmware Engineer", "軟體測試人員", "QA Engineer",
            "BIOS工程師", "BIOS Engineer", "CIM工程師", "MES工程師",
            "網站程式設計師", "Web Developer", "System Analyst", "系統分析師",
            "System Engineer", "系統工程", "MIS", "網路管理工程師",
            "數據科學家", "資料科學家", "Data Scientist", "數據分析師", "資料分析師",
            "Data Analyst", "數據架構師", "Data Architect", "數據工程師", 
            "資料工程師", "Data Engineer", "機器學習工程師", "Machine Learning Engineer",
            "演算法工程師", "Algorithm engineer", "資料庫管理人員", "DBA",
            "雲端架構師", "Cloud Architect", "雲端工程師", "Cloud Engineer",
            "雲端資安", "雲端網路工程師", "Network Engineer", "資訊安全架構",
            "資安工程師", "Cybersecurity Engineer", "資安滲透工程師",
            "前端工程師", "Frontend Engineer", "後端工程師", "Backend Engineer",
            "全端工程師", "Full Stack Engineer", "DevOps工程師", "區塊鏈工程師",
            "Blockchain Engineer", "嵌入式工程師", "Embedded Software Engineer",
            "自動化測試工程師", "Automation QA Engineer", "APP工程師"
        ]
        
        # 計算需要多少chunks
        chunks = []
        for i in range(0, len(key_texts), max_keywords_per_chunk):
            chunk = key_texts[i:i + max_keywords_per_chunk]
            chunks.append(chunk)
            
        logging.info(f"關鍵字已分割為 {len(chunks)} 個chunks，每個chunk包含關鍵字: ")
        for i, chunk in enumerate(chunks):
            logging.info(f"Chunk {i}: {chunk}")
            
        return chunks
        
    except Exception as e:
        logging.error(f"分割關鍵字時發生錯誤: {str(e)}")
        raise

@contextmanager
def timeout(minutes=1):
    """Context manager for timing out a block of code"""
    def timeout_handler(signum, frame):
        raise TimeoutError(f"Operation timed out after {minutes} minutes")
        
    # Set the timeout handler
    signal.signal(signal.SIGALRM, timeout_handler)
    signal.alarm(int(minutes * 60))
    
    try:
        yield
    finally:
        # Disable the alarm
        signal.alarm(0)

class JobScraper:
    def __init__(self, max_retries=3, retry_delay=5, chunk_id=None):
        """初始化爬蟲類"""
        self.logger = logging.getLogger(f"{__name__}_{chunk_id}" if chunk_id else __name__)
        self.setup_logging()
        
        self.today = date.today()
        # 直接使用chunk_id作為檔案名稱的一部分
        self.chunk_id = chunk_id
        self.output_dir = Path("/opt/airflow/jobs_csv")
        self.output_dir.mkdir(parents=True, exist_ok=True)
        # os.chmod(self.output_dir, 0o755)
        
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.session = self.create_retry_session(retries=self.max_retries, 
                                               backoff_factor=self.retry_delay)
        
    def setup_logging(self):
        """設置日誌配置"""
        logging.basicConfig(
            filename='scraping.log',
            level=logging.INFO,
            format='%(asctime)s:%(levelname)s:%(message)s',
            encoding='utf-8'
        )

    def get_random_headers(self):
        """返回HTTP頭"""
        return {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36 Edg/130.0.0.0',
        }

    def get_total_pages(self, key):
        """獲取總頁數"""
        try:
            url = f"https://www.104.com.tw/jobs/search/?ro=0&kwop=7&keyword={key}&order=15&asc=0&page=1&mode=s&jobsource=m_joblist_search&searchTempExclude=2"
            logging.info(f"請求URL: {url}")
            response = requests.get(url)
            response.raise_for_status()
            soup = BeautifulSoup(response.content, "html.parser")
            page_select = soup.select_one('div.high-light.multiselect__content-wrapper > ul > li:nth-child(1) > span')
            text = page_select.text
            txtre = re.search(r'/\s*(\d+)', text)
            page = txtre.group(1)
            return int(page)
        except requests.exceptions.HTTPError as e:
            logging.error(f"獲取頁數時發生錯誤: {e}")
            return 1

    def scrape_jobs(self, key_txt, batch_size=30):
        """爬取工作信息並保存到CSV"""
        key = quote(key_txt)
        # 簡化檔案名稱格式
        filename = f"chunk_{self.chunk_id}_{key_txt}.csv"
        path_csv = self.output_dir / filename
        
        logging.info(f"開始爬取關鍵字: {key_txt}, 輸出檔案: {path_csv}")

        # 創建CSV文件並寫入標題
        with open(path_csv, mode='a+', newline='', encoding='utf-8') as employee_file:
            employee_writer = csv.writer(
                employee_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
            employee_writer.writerow(['日期', '工作名稱', '公司名稱', '職務類別', '薪資', '工作內容', '地區',
                                    '經歷', '學歷', '兩周內應徵人數', '文章編號', '工作網址', '產業別', 
                                    '擅長工具', '工作技能', '科系要求', '其他條件'])

        try:
            total_pages = self.get_total_pages(key)
            get_sum_page = min(total_pages, 150)  # 限制最多爬取150頁
            logging.info(f'開始爬取關鍵字: {key_txt}, 共有：{get_sum_page} 頁')

            # 分批處理頁面
            for start_page in range(1, get_sum_page + 1, batch_size):
                end_page = min(start_page + batch_size - 1, get_sum_page)
                logging.info(f'處理第 {start_page} 到 {end_page} 頁')
                
                for page in tqdm(range(start_page, end_page + 1), 
                               desc=f"爬取 {key_txt} ({start_page}-{end_page})"):
                    # 每頁隨機延遲1-5秒
                    sleep_time = random.uniform(1, 5)
                    logging.info(f"頁面 {page} 延遲 {sleep_time:.2f} 秒")
                    time.sleep(sleep_time)
                    
                    url = f"https://www.104.com.tw/jobs/search/?ro=0&kwop=7&keyword={key}&order=15&asc=0&page={page}&mode=s&jobsource=m_joblist_search&searchTempExclude=2"
                    resp = requests.get(url)
                    soup = BeautifulSoup(resp.content, "html.parser")
                    
                    if soup is None:
                        continue
                        
                    job_elements = soup.select('.job-summary')

                    for title_1 in job_elements:
                        try:
                            # 提取各種職位信息
                            date = '廣告' if title_1.select('.col-auto.date')[0].select('i') else title_1.select('.col-auto.date')[0].get_text()
                            area = title_1.select('.info-tags.gray-deep-dark')[0].find('a').get_text()
                            experience = title_1.select('.info-tags.gray-deep-dark')[0].find_all('a')[1].get_text()
                            education = title_1.select('.info-tags.gray-deep-dark')[0].find_all('a')[2].get_text() if len(title_1.select('.info-tags.gray-deep-dark')[0].find_all('a')) > 2 else ""
                            title_url = title_1.select('.info-job__text.jb-link.jb-link-blue.jb-link-blue--visited.h2')[0]['href']
                            title_str = title_url.split('?')[0].split('/')[-1]
                            title = title_1.select('.info-job__text.jb-link.jb-link-blue.jb-link-blue--visited.h2')[0]['title']
                            company_name = title_1.select('.info-company__text.jb-link.jb-link-blue.jb-link-blue--visited.h4')[0].get_text()

                            salary_element = title_1.select('.info-tags.gray-deep-dark')[0].find_all('a')
                            salary = salary_element[3].get_text() if len(salary_element) > 3 else "待遇面議"
                            if salary != "待遇面議":
                                salary = re.search(r'\d+.\d+', salary).group() if re.search(r'\d+.\d+', salary) else "0"

                            people = title_1.select('.action-apply__range.d-flex.text-center.align-items-center')[0]['title'] if title_1.select('.action-apply__range.d-flex.text-center.align-items-center') else ""
                            industry = title_1.select('.info-company-addon-type.text-gray-darker.font-weight-bold')[0].get_text()
                            industry = industry if "業" in industry else "無"

                            # 取工作網址內的內容
                            detail_soup = self.read_url(title_url)
                            introduction, tools, skills, job_categories, major_requirement, other_requirements = self.extract_job_details(detail_soup)

                            # 每個職位詳情頁面隨機延遲1-5秒
                            sleep_time = random.uniform(1, 5) 
                            logging.info(f"職位詳情延遲 {sleep_time:.2f} 秒")
                            time.sleep(sleep_time)

                            # 準備寫入CSV的行數據
                            row = [date, title, company_name, job_categories, salary, introduction, area, experience, education,
                                   people, title_str, title_url, industry, tools, skills, major_requirement, other_requirements]

                            # 數據寫入CSV文件
                            with open(path_csv, mode='a', newline='', encoding='utf-8') as employee_file:
                                employee_writer = csv.writer(
                                    employee_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
                                employee_writer.writerow(row)
                        except Exception as e:
                            logging.error(f"處理職位資訊發生錯誤: {e}")
                            continue
                
                # 每批次完成後短暫休息
                time.sleep(random.uniform(2, 5))

            logging.info(f"完成爬取關鍵字: {key_txt}")
        except Exception as e:
            logging.error(f"爬蟲過程中發生錯誤: {e}")
            raise

    def read_url(self, url):
        """讀取URL並返回BeautifulSoup對象"""
        try:
            response = self.session.get(url)
            response.raise_for_status()
            return BeautifulSoup(response.text, 'html.parser')
        except requests.exceptions.RequestException as e:
            logging.error(f"讀取URL失敗: {e}")
            return None

    @staticmethod
    def create_retry_session(retries=5, backoff_factor=0.5):  # 增加重試次數和等待時間
        """創建具有重試機制的會話"""
        retry_strategy = Retry(
            total=retries,
            backoff_factor=backoff_factor,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["HEAD", "GET", "OPTIONS", "POST", "PUT", "DELETE"],
            respect_retry_after_header=True
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session = requests.Session()
        session.mount("https://", adapter)
        session.mount("http://", adapter)
        return session

    def extract_job_details(self, detail_soup):
        """從詳細頁面提取工作細節"""
        try:
            # 添加隨機延遲 1-7 秒
            time.sleep(random.uniform(1, 7))
            
            introduction = ""
            tools = []
            skills = []
            job_categories = []
            major_requirement = "未指定"
            other_requirements = "不拘"

            if detail_soup:
                # 提取工作內容
                job_description = detail_soup.select_one('p.job-description__content')
                if job_description:
                    introduction = job_description.get_text(strip=True)
                else:
                    introduction = "未提供工作內容"

                # 提取其他詳細信息
                list_rows = detail_soup.select('div.list-row.row.mb-2')
                for row in list_rows:
                    header = row.select_one('div.col.p-0.mr-4.list-row__head h3')
                    if header:
                        header_text = header.get_text().strip()
                        data_block = row.select_one('div.col.p-0.list-row__data')

                        if data_block:
                            if "擅常工具" in header_text:
                                tools = [u.get_text().strip() for u in data_block.select('a.tools u')]
                            elif "工作技能" in header_text:
                                skills = [u.get_text().strip() for u in data_block.select('a.skills u')]
                            elif "職務類別" in header_text:
                                job_categories = [u.get_text().strip() for u in data_block.select('u')]
                            elif "科系要求" in header_text:
                                major_element = data_block.select_one('div.t3.mb-0')
                                major_requirement = major_element.get_text(strip=True) if major_element else "未指定"
                            elif "其他條件" in header_text:
                                other_element = (data_block.select_one('div.col.p-0.job-requirement-table__data p.t3.m-0') 
                                               or data_block.select_one('div.t3.mb-0'))
                                other_requirements = other_element.get_text(strip=True) if other_element else "不拘"

                # 將列表轉換為字符串
                tools = ', '.join(tools) if tools else "不拘"
                skills = ', '.join(skills) if skills else "不拘"
                job_categories = ', '.join(job_categories) if job_categories else "未指定"
                other_requirements = "不拘" if other_requirements.strip() in ["", "未填寫"] else other_requirements

            return introduction, tools, skills, job_categories, major_requirement, other_requirements
        
        except Exception as e:
            logging.error(f"提取工作詳細信息時發生錯誤: {e}")
            # 發生錯誤時返回預設值
            return "未提供工作內容", "不拘", "不拘", "未指定", "未指定", "不拘"

def web_crawler(key_texts_chunk, chunk_index, **context):
    """執行爬蟲任務"""
    if not key_texts_chunk:
        raise ValueError("未提供關鍵字列表")
        
    logging.info(f"Chunk {chunk_index} 開始處理關鍵字: {key_texts_chunk}")
    
    try:
        # 創建帶有chunk_id的scraper實例
        scraper = JobScraper(max_retries=3, retry_delay=5, chunk_id=chunk_index)
        results = []
        
        for i, key_txt in enumerate(key_texts_chunk):
            try:
                # 添加進度日誌
                logging.info(f"處理進度: {i+1}/{len(key_texts_chunk)} - 關鍵字: {key_txt}")
                
                scraper.scrape_jobs(key_txt)
                results.append({
                    'keyword': key_txt,
                    'status': 'success',
                    'timestamp': datetime.now().isoformat()
                })
                
                # 添加詳細日誌
                logging.info(f"Chunk {chunk_index} 成功處理關鍵字: {key_txt}")
                    
            except Exception as e:
                error_msg = f"Chunk {chunk_index} 處理關鍵字 {key_txt} 時發生錯誤: {str(e)}"
                logging.error(error_msg)
                results.append({
                    'keyword': key_txt,
                    'status': 'error',
                    'error': str(e),
                    'timestamp': datetime.now().isoformat()
                })
                continue
                
        return results
        
    except Exception as e:
        error_msg = f"Chunk {chunk_index} 執行錯誤: {str(e)}"
        logging.error(error_msg)
        raise AirflowException(error_msg)

if __name__ == "__main__":
    web_crawler()
