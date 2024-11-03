# -*- coding: utf-8 -*-
"""「csv_to _big_query.ipynb」

Automatically generated by Colab.

Original file is located at
    https://colab.research.google.com/drive/1mXv2AobrppwN0saVXiwt9H_o2fOjBG9C
"""
import jieba
import pandas as pd
from pathlib import Path
import logging
import os
import numpy as np
from datetime import datetime

class DataProcessor:
    def __init__(self):
        self.setup_logging()
        # 從環境變數讀取配置，如果沒有則使用預設值
        self.input_dir = Path(os.getenv('AIRFLOW_VAR_INPUT_DIR', 'jobs_csv'))
        self.output_file = os.getenv('AIRFLOW_VAR_OUTPUT_FILE', '104data_cleaning.csv')
        self.resources = self.load_resources()
        logging.info(f"初始化完成，輸入目錄: {self.input_dir}")

    def setup_logging(self):
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s:%(levelname)s:%(message)s'
        )
    
    def load_resources(self):
        """載入所有需要的資源文件"""
        try:
            resources = {}
            # 從環境變數讀取資源目錄路徑
            base_path = Path(os.getenv('AIRFLOW_VAR_RESOURCES_PATH', 
                           Path(__file__).parent.parent / 'resources'))
            
            # 載入結巴分詞字典
            dict_path = base_path / 'dict.txt'
            if dict_path.exists():
                jieba.load_userdict(str(dict_path))
                logging.info("已載入自定義字典")
            
            # 載入停用詞
            stop_words_path = base_path / 'stop_words.txt'
            if stop_words_path.exists():
                with open(stop_words_path, 'r', encoding='utf-8') as f:
                    resources['stop_words'] = set([line.strip() for line in f])
                logging.info("已載入停用詞")
            
            # 載入詳細詞彙表
            detail_words_path = base_path / 'lowercase_detail_word.txt'
            if detail_words_path.exists():
                with open(detail_words_path, 'r', encoding='utf-8') as f:
                    resources['detail_words'] = set([line.strip() for line in f])
                logging.info("已載入詳細詞彙表")
            
            return resources
            
        except Exception as e:
            logging.error(f"載入資源文件時發生錯誤: {e}")
            raise

    def read_csv_files(self):
        try:
            all_data = pd.DataFrame()
            for csv_file in self.input_dir.glob('*.csv'):
                logging.info(f"正在處理文件: {csv_file}")
                try:
                    df = pd.read_csv(csv_file)
                    all_data = pd.concat([all_data, df], ignore_index=True)
                except Exception as e:
                    logging.error(f"處理文件 {csv_file} 時發生錯誤: {e}")
                    continue
            return all_data
        except Exception as e:
            logging.error(f"讀取CSV文件時發生錯誤: {e}")
            raise

    def process_data(self):
        try:
            df = self.read_csv_files()
            if df.empty:
                logging.error("沒有找到任何CSV文件或所有文件都是空的")
                return

            # Check if '日期' column exists and rename it to 'report_date'
            if '日期' in df.columns:
                df = df.rename(columns={'日期': 'report_date'})
            else:
                # If no date column exists, add current date
                logging.warning("找不到日期欄位，使用當前日期")
                df['report_date'] = datetime.today().strftime('%Y-%m-%d')

            # 處理 report_date 欄位
            df['report_date'] = df['report_date'].apply(format_report_date)
            logging.info("已完成 report_date 欄位格式化")

            # 使用已載入的詳細詞彙表
            detail_words = self.resources.get('detail_words', set())
            
            def get_job_skills(text):
                if pd.isna(text):
                    return ''
                seg_words_list = jieba.lcut(str(text))
                word_select_list = set()
                
                for term in seg_words_list:
                    term = term.lower()
                    if term in detail_words:
                        word_select_list.add(term)
                
                return ",".join(word_select_list)

            # 拆解工作內容詳細
            df["工作技能分割1"] = df["工作內容"].apply(get_job_skills)
            df["工作技能分割2"] = df["其他條件"].apply(get_job_skills)

            # 工作內容詳細，工作內容簡,附加條件,電腦專長合併成同一欄
            df['擅長工具'] = df['擅長工具'].str.lower()
            
            # 處理 "不拘" 的情況
            df['擅長工具'] = df['擅長工具'].replace('不拘', '不拘,')
            
            # 合併欄位
            df['擅長工具合併後'] = df['工作技能分割1'].fillna('') + ',' + df['工作技能分割2'].fillna('') + ',' + df['擅長工具'].fillna('')

            # 處理工具欄位
            if '擅長工具合併後' in df.columns:
                # 分割字串
                df['擅長工具合併後'] = df['擅長工具合併後'].str.split(',')
                
                # 清理空值和開頭的空字串，但保留所有有效值
                df['擅長工具合併後'] = df['擅長工具合併後'].apply(
                    lambda x: [item.strip() for item in x if item.strip()] if isinstance(x, list) else []
                )
                
                # 展開列表
                df = df.explode('擅長工具合併後')
                
                # 確保空值被替換為 "不拘"
                df['擅長工具合併後'] = df['擅長工具合併後'].replace('', None)
                df['擅長工具合併後'] = df['擅長工具合併後'].fillna('不拘')

                # 處理職務類別
                if '職務類別' in df.columns:
                    df['職務類別'] = df['職務類別'].str.split(',')
                    df['職務類別'] = df['職務類別'].apply(
                        lambda x: [item.strip() for item in x if item.strip()] if isinstance(x, list) else []
                    )
                    df = df.explode('職務類別')

                # 處理工作技能
                if '工作技能' in df.columns:
                    df['工作技能'] = df['工作技能'].str.split(',')
                    df['工作技能'] = df['工作技能'].apply(
                        lambda x: [item.strip() for item in x if item.strip()] if isinstance(x, list) else []
                    )
                    df = df.explode('工作技能')

            # 去除過渡的欄位
            columns_to_drop = [
                '工作技能分割1', 
                '工作技能分割2', 
                "其他條件", 
                "兩周內應徵人數",
                "文章編號", 
                "工作內容", 
                "擅長工具",
                "科系要求",
                "學歷"
            ]
            
            # 只刪除存在的欄位
            existing_columns = [col for col in columns_to_drop if col in df.columns]
            if existing_columns:
                df.drop(columns=existing_columns, inplace=True)
                logging.info(f"已刪除以下欄位: {existing_columns}")
            
            # 先處理非數值欄位的空值
            for col in df.columns:
                if col != '薪資':  # 排除薪資欄位
                    df[col] = df[col].fillna('')

            # 單獨處理薪資欄位
            if '薪資' in df.columns:
                # 將待遇面議轉為 NaN
                df['薪資'] = df['薪資'].replace('待遇面議', np.nan)
                
                # 移除數字中的逗號並轉換為浮點數
                df['薪資'] = (df['薪資']
                           .astype(str)
                           .str.replace(',', '', regex=False)  # 移除逗號
                           .replace('', np.nan)  # 空字串轉為 NaN
                           .apply(lambda x: float(x) if pd.notna(x) else x))  # 安全轉換
                
                # 轉換為可空的整數類型
                df['薪資'] = df['薪資'].astype('float')
                
                logging.info("薪資欄位處理完成")

            # 替換不需要的特殊字元
            special_chars = {
                r'\(': '', r'\)': '',
                r'\【': '', r'\】': '',
                r'\「': '', r'\」': '',
                r'\,': '', r'\r': '',
                r'\n': '', r'\s': '',
                r'\t': '', r'待遇面議': ''
            }

            # 使用 replace 批次處理特殊字元
            for old, new in special_chars.items():
                df = df.replace(old, new, regex=True)

            # 去掉欄位名稱中的特殊字元
            df.columns = df.columns.str.replace(r'[()\s]', '', regex=True)

            # 定義中文欄位到英文欄位的映射字典
            column_mapping = {
                # "日期": "report_date",
                "工作名稱": "job_title",
                "公司名稱": "company_name",
                "職務類別": "job_category",
                "薪資": "salary",
                "地區": "location_region",
                "經歷": "experience",
                "產業別": "industry",
                '工作網址': 'job_url',
                "工作技能": "job_skills",
                "擅長工具合併後": "tools"
            }

            # 重命名 DataFrame 欄位
            df = df.rename(columns=column_mapping)
            
            # 確保欄位順序
            desired_order = [
                "report_date", "job_title", "company_name",
                "main_category", "sub_category", "job_category", 
                "salary", "location_region", "experience",
                "industry", "job_url", "job_skills", "tools"
            ]
            
            # 重新排序欄位
            df = df[desired_order]

            # 處理日期格式
            df['report_date'] = df['report_date'].apply(format_report_date)
            logging.info("已完成 report_date 欄位格式化")

            # 建立輸出目錄
            output_dir = Path(__file__).parent.parent / 'data'
            output_dir.mkdir(exist_ok=True)

            # 設定輸出檔案路徑
            output_file = output_dir / '104data.cleaning.csv'

            # 保存處理後的數據
            df.to_csv(output_file, index=False)
            logging.info(f"數據處理完成，已保存至: {output_file}")

            return df

        except Exception as e:
            logging.error(f"數據處理過程中發生錯誤: {e}")
            raise

def format_report_date(date_str):
    try:
        # 如果是 "廣告" 則返回今天日期
        if date_str == "廣告":
            return datetime.today().strftime('%Y-%m-%d')
            
        # 處理帶有 "企" 字的日期
        if '企' in str(date_str):
            date_str = str(date_str).replace('企', '')
            
        # 處理只有月日的情況 (例如: "4/18", "10/13")
        if '/' in str(date_str):
            month, day = date_str.split('/')
            current_year = datetime.today().year
            return f"{current_year}-{int(month):02d}-{int(day):02d}"
            
        return date_str
        
    except Exception as e:
        logging.warning(f"日期格式化錯誤: {e}, 使用今天日期")
        return datetime.today().strftime('%Y-%m-%d')

def clean_main():
    try:
        processor = DataProcessor()
        processor.process_data()
    except Exception as e:
        logging.error(f"主程序執行錯誤: {e}")
        raise

if __name__ == "__main__":
    clean_main()