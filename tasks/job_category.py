import pandas as pd
import os
from pathlib import Path
import glob

# 從環境變數讀取配置
JOBS_CSV_DIR = os.getenv('AIRFLOW_VAR_JOBS_CSV_DIR', 'jobs_csv')
OUTPUT_ENCODING = os.getenv('AIRFLOW_VAR_OUTPUT_ENCODING', 'utf-8-sig')

# 大分類字典
category_dict = {
    "軟體工程": ['軟體工程師', 'Software Developer', '通訊軟體工程師', '韌體工程師', 'Firmware Engineer', '軟體測試人員', 'QA Engineer', 'BIOS工程師', 'BIOS Engineer', 'CIM工程師', 'MES工程師', '網站程式設計師', 'Web Developer'],
    "系統規劃": ['System Analyst', '系統分析師', 'System Engineer', '系統工程師'],
    "網路管理": ['MIS', '網路管理工程師'],
    "資料科學": ['數據科學家', '資料科學家', 'Data Scientist', '數據分析師', '資料分析師', 'Data Analyst', '數據架構師', 'Data Architect', '數據工程師', '資料工程師', 'Data Engineer', '機器學習工程師', 'Machine Learning Engineer', '演算法工程師', 'Algorithm engineer', '資料庫管理人員', 'DBA'],
    "雲服務/雲計算": ['雲端架構師', 'Cloud Architect', '雲端工程師', 'Cloud Engineer', '雲端資安', '雲端網路工程師', 'Network Engineer'],
    "資訊安全": ['資訊安全架構', '資安工程師', 'Cybersecurity Engineer', '資安滲透工程師'],
    "系統發展": ['前端工程師', 'Frontend Engineer', '後端工程師', 'Backend Engineer', '全端工程師', 'Full Stack Engineer', 'DevOps工程師', '區塊鏈工程師', 'Blockchain Engineer', '嵌入式工程師', 'Embedded Software Engineer', '自動化測試工程師', 'Automation QA Engineer', 'APP工程師']
}

# 小分類字典
subcategory_dict = {
    "軟體工程師": ['軟體工程師', 'Software Developer'],
    "通訊軟體工程師": ['通訊軟體工程師'],
    "韌體工程師": ['韌體工程師', 'Firmware Engineer'],
    "軟/韌體測試人員": ['軟體測試人員', 'QA Engineer'],
    "BIOS工程師": ['BIOS工程師', 'BIOS Engineer'],
    "CIM工程師": ['CIM工程師'],
    "MES工程師": ['MES工程師'],
    "網站程式設計師": ['網站程式設計師', 'Web Developer'],
    "系統分析師": ['System Analyst', '系統分析師'],
    "系統工程師": ['System Engineer', '系統工程師'],
    "網路管理工程師": ['MIS', '網路管理工程師'],
    "數據科學家": ['數據科學家', '資料科學家', 'Data Scientist'],
    "數據分析師": ['數據分析師', '資料分析師', 'Data Analyst'],
    "數據架構師": ['數據架構師', 'Data Architect'],
    "數據工程師": ['數據工程師', '資料工程師', 'Data Engineer'],
    "機器學習工程師": ['機器學習工程師', 'Machine Learning Engineer'],
    "演算法工程師": ['演算法工程師', 'Algorithm engineer'],
    "資料庫管理人員": ['資料庫管理人員', 'DBA'],
    "雲端架構師": ['雲端架構師', 'Cloud Architect'],
    "雲端工程師": ['雲端工程師', 'Cloud Engineer'],
    "雲端資安": ['雲端資安'],
    "雲端網路工程師": ['雲端網路工程師', 'Network Engineer'],
    "資訊安全架構": ['資訊安全架構'],
    "資安工程師": ['資安工程師', 'Cybersecurity Engineer'],
    "資安滲透工程師": ['資安滲透工程師'],
    "前端工程師": ['前端工程師', 'Frontend Engineer'],
    "後端工程師": ['後端工程師', 'Backend Engineer'],
    "全端工程師": ['全端工程師', 'Full Stack Engineer'],
    "DevOps工程師": ['DevOps工程師'],
    "區塊鏈工程師": ['區塊鏈工程師', 'Blockchain Engineer'],
    "嵌入式工程師": ['嵌入式工程師', 'Embedded Software Engineer'],
    "自動化測試工程師": ['自動化測試工程師', 'Automation QA Engineer'],
    "APP工程師": ['APP工程師']
}

def categorize_jobs():
    """讀取 jobs_csv 目錄中的所有 CSV 檔案，根據檔名添加分類欄位"""
    try:
        # 設定目錄路徑
        directory = Path(JOBS_CSV_DIR)
        
        # 使用 glob 取得目錄中所有 CSV 檔案
        csv_files = glob.glob(str(directory / '*.csv'))
        
        if not csv_files:
            print(f"錯誤: 在 '{directory}' 目錄中找不到 CSV 檔案")
            return
            
        # 處理每個 CSV 檔案
        for csv_file in csv_files:
            print(f"處理檔案: {csv_file}")
            
            # 取得檔案名稱（不含路徑和副檔名）
            file_name = os.path.splitext(os.path.basename(csv_file))[0]
            
            # 讀取 CSV 檔案
            df = pd.read_csv(csv_file)
            
            # 根據檔名判斷子分類
            df['sub_category'] = next(
                (sub_cat for sub_cat, sub_keywords in subcategory_dict.items()
                 if any(keyword.lower() in file_name.lower() for keyword in sub_keywords)),
                pd.NA
            )
            
            # 如果有子分類，判斷主分類
            if pd.notna(df['sub_category'].iloc[0]):
                df['main_category'] = next(
                    (main_cat for main_cat, keywords in category_dict.items()
                     if df['sub_category'].iloc[0] in keywords),
                    pd.NA
                )
            else:
                df['main_category'] = pd.NA
            
            # 保存回原檔案
            df.to_csv(csv_file, index=False, encoding=OUTPUT_ENCODING)
            print(f"已完成檔案 {csv_file} 的分類")
            
    except Exception as e:
        print(f"處理檔案時發生錯誤: {e}")
        raise

if __name__ == "__main__":
    categorize_jobs()