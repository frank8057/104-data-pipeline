# 104 Job Market Data Pipeline

An automated ETL pipeline for collecting and analyzing job market data from 104 Job Bank, powered by Apache Airflow.


## 📁 Project Structure

<pre>
/airflow
├── dags/               # Airflow DAG definitions
│   ├── test.py         # Main workflow
│   └── dbt_refresh_dag.py  # DBT refresh workflow
├── tasks/              # Task modules
│   ├── __init__.py
│   ├── bigquery_load.py # BigQuery data loading
│   ├── data_clean.py    # Data cleaning
│   ├── gcs_upload.py    # GCS file upload
│   ├── job_category.py  # Job classification
│   └── main_scrap.py    # Web scraping
├── utils/              # Utility functions
│   └── __init__.py
├── resources/          # Resource files
│   └── lowercase_detail_word.txt # Keyword list
├── data/               # Data storage
│   └── 104data.cleaning.csv # Cleaned data
├── .gitignore          # Git ignore rules
├── dbt_project/        # DBT project files
│   ├── models/         # DBT models
│   └── dbt_project.yml # DBT config
└── README.md           # Project documentation
</pre>

## 🔄 Pipeline Flow

| Step       | Description                 | Script               |
|------------|-----------------------------|----------------------|
| 🌐 Source  | 104 Website                 | -                    |
| 🔍 Scraper | Web Scraper                 | `main_scrap.py`      |
| 🗂️ Categorize | Job Categorization        | `job_category.py`    |
| 🧹 Clean   | Data Cleaning               | `data_clean.py`      |
| ☁️ Upload  | GCS Upload                  | `gcs_upload.py`      |
| 🗄️ Load    | BigQuery Load               | `bigquery_load.py`   |
| 🔄 Transform| DBT Data Transformation     | `dbt_refresh_dag.py` |
| 📊 Visualize | Data Analysis & Visualization | -               |


## 🌟 Key Features
- 💼 Automated job data collection
- 🧹 Intelligent data cleaning
- 📊 Job classification and analysis
- ☁️ Cloud storage integration
- 📝 Comprehensive error handling
- ⚙️ Flexible configuration options

## 🛠 Tech Stack
- 🐍 Python: Core development
- 🌪 Apache Airflow: Workflow management
- ☁️ Google Cloud Platform:
  - 📦 Cloud Storage: Data storage
  - 📊 BigQuery: Data warehousing
- 🕷 BeautifulSoup4: Web parsing
- 🐼 Pandas: Data processing
- 🔄 DBT: Data transformation

## 🚀 Getting Started

### Prerequisites
- Python >= 3.8
- Apache Airflow >= 2.7.1
- GCP Account with enabled services
- Stable internet connection
- Minimum 8GB RAM
- Access to a Google Cloud Platform (GCP) Virtual Machine (VM) with SSH capabilities

### Installation
1. Clone the repository:
\`\`\`bash
git clone https://github.com/frank8057/104-data-pipeline.git
\`\`\`

2. Install dependencies:
\`\`\`bash
pip install -r requirements.txt
\`\`\`

3. Configure GCP credentials:
\`\`\`bash
export GOOGLE_APPLICATION_CREDENTIALS="your-credentials.json"
\`\`\`

4. Configure DBT:
\`\`\`bash
cd airflow/dbt_project
dbt deps
dbt debug
\`\`\`

### System Startup
1. Initialize Airflow:
\`\`\`bash
airflow webserver -p 8080
airflow scheduler
\`\`\`

2. Access web interface: http://localhost:8080

## 📋 Data Processing

### 1️⃣ Data Collection
- **Automated Web Scraping**: Uses `main_scrap.py` to scrape job listings from the 104 Job Bank.
  - **Keyword Splitting**: Divides keywords into smaller chunks for efficient processing.
  - **Retry Mechanism**: Implements retries and backoff strategies for robust scraping.
  - **Timeout Handling**: Ensures each keyword is processed within a specified time limit.
  - **Random Delays**: Introduces random delays to mimic human behavior and avoid detection.

### 2️⃣ Data Cleaning
- **Data Consolidation**: Uses `data_clean.py` to merge and clean data from multiple CSV files.
  - **Custom Dictionary**: Utilizes Jieba with a custom dictionary for precise text segmentation.
  - **Skill Extraction**: Extracts relevant skills and tools from job descriptions and requirements.
  - **Data Normalization**: Standardizes date formats and handles missing values.
  - **Column Mapping**: Renames and reorders columns for consistency and clarity.

### 3️⃣ Data Analysis
- **Job Classification**: Categorizes jobs based on extracted skills and job titles.
- **Salary Range Analysis**: Analyzes salary data to provide insights into market trends.
- **Skill Requirement Statistics**: Aggregates data on required skills and tools.

### 4️⃣ Data Storage
- **Cloud Backup**: Stores cleaned data in Google Cloud Storage for durability.
- **Data Warehouse Integration**: Loads data into BigQuery for advanced querying and analysis.
- **Query Optimization**: Implements partitioning and indexing strategies for efficient data retrieval.

### 4️⃣ Data Transformation
- **DBT Models**: Uses DBT for data modeling and transformation
  - **Incremental Processing**: Handles incremental data loads
  - **Data Quality Tests**: Implements data quality checks
  - **Documentation**: Auto-generates data documentation

## 📈 Monitoring

### DAG Overview
- **DAG Name**: `job_scraping_pipeline`
  - **Description**: Automates the ETL process for scraping job listings from 104 Job Bank.
  - **Schedule**: Runs every 7 days at midnight.
  - **Timeout**: Each DAG run has a timeout of 8 hours to ensure timely completion.

### Task Groups
- **Scraping Tasks**: 
  - **Function**: Executes web scraping for different keyword chunks.
  - **Concurrency**: Limited to 6 concurrent tasks to manage resource usage.
  - **Retries**: Each task retries up to 3 times with a 5-minute delay between attempts.

- **Data Processing Tasks**:
  - **Categorize Jobs**: Classifies job listings into categories.
  - **Clean Data**: Cleans and processes raw data for analysis.
  - **Upload to GCS**: Uploads cleaned data to Google Cloud Storage.
  - **Dependencies**: Tasks are executed sequentially to ensure data integrity.

- **BigQuery Load Task**:
  - **Function**: Loads processed data into BigQuery for analysis.
  - **Execution Timeout**: Set to 2 hours to handle large data volumes.

- **DBT Transform Task**:
  - **Function**: Runs DBT models for data transformation
  - **Execution Timeout**: Set to 1 hour to handle large data volumes

### Logging and Error Handling
- **Logging**: 
  - Logs are configured to capture detailed information about each task's execution.
  - Includes timestamps, task IDs, and error messages for troubleshooting.

- **Error Handling**:
  - Tasks are wrapped in try-except blocks to catch and log exceptions.
  - Airflow's retry mechanism is used to handle transient failures.

### Environment Information
- **Python Path**: Logs the Python interpreter path for debugging.
- **DAG ID**: Logs the DAG ID to track execution in the Airflow UI.

### Performance Metrics
- **Task Duration**: Monitors the execution time of each task to identify bottlenecks.
- **Success Rate**: Tracks the success rate of tasks to ensure reliability.
- **Resource Usage**: Monitors CPU and memory usage to optimize performance.

## ⚠️ Important Notes
- Respect 104 website's robots.txt
- Regular keyword updates
- Monitor cloud usage

## 💡 Additional Information

### Data Schema
- report_date: Date of the report
- job_title: Position name
- company_name: Employer
- main_category: Main job category
- sub_category: Sub job category
- job_category: Job classification
- salary: Salary offered
- location_region: Region of the job location
- experience: Required experience
- industry: Industry type
- job_url: URL of the job listing
- job_skills: Skills required for the job
- tools: Tools required for the job
- insert_timestamp: Timestamp of data insertion (used for partitioning)

### Common Issues
1. Modifying scraping keywords:
   - Edit resources/lowercase_detail_word.txt
   - Customize keyword list

2. Adjusting scraping frequency:
   - Modify DAG schedule
   - Default: Daily update

3. Data storage locations:
   - CSV: data/104data.cleaning.csv
   - GCS: {project-id}-104-job-data
   - BigQuery: job_data_dataset.job_table

### Performance Guidelines
- Scraping interval: 5-10 seconds
- Batch size: 500-1000 records
- Update schedule: Off-peak hours
- BigQuery optimization:
  - Date partitioning
  - Proper indexing
  - Regular maintenance

### Development Roadmap
- [ ] Job trend analysis
- [ ] Enhanced data cleaning
- [ ] Salary prediction model
- [ ] Extended job categories

### Dependencies
- Python >= 3.8
- Apache Airflow >= 2.7.1
- pandas >= 1.5.0
- google-cloud-storage >= 2.10.0
- google-cloud-bigquery >= 3.11.0
- beautifulsoup4 >= 4.12.0
- requests >= 2.31.0
- dbt-core >= 1.5.0
- dbt-bigquery >= 1.5.0

### System Requirements
- Linux/macOS/Windows WSL2
- Docker (optional)
- GCP Account
- 2core, 8GB+ RAM
- Stable network connection

### Environment Setup
- The pipeline is deployed on a Google Cloud Platform (GCP) Virtual Machine (VM).
- Secure Shell (SSH) is used for remote access and management of the VM.
- Ensure that the VM has the necessary permissions and access to GCP services like BigQuery and Cloud Storage.

### Performance Metrics
- Average scraping rate: ~0.1 pages/second (approximately 1 page every 10 seconds)
- Data cleaning rate: 60 records/minute
- Typical daily volume: 5000-10000 records
- Storage requirement: ~1.5GB to 3GB/month

### Version History
- 2024/11: Initial Release
  - Basic scraping functionality
  - Data cleaning pipeline
  - GCP integration
  - Airflow DAG setup
  - BigQuery warehouse integration
- 2024/11: v1.1
  - Added DBT integration
  - Enhanced data transformation pipeline
  - Improved documentation

## 🤝 Contributing
Contributions are welcome!
- Submit issues for bug reports
- Create pull requests for improvements

## 📚 Useful Links
- [Apache Airflow Documentation](https://airflow.apache.org/docs/)
- [Google Cloud Documentation](https://cloud.google.com/docs)
- [104 Job Bank](https://www.104.com.tw)
- [Project Wiki](https://github.com/frank8057/104-data-pipeline/wiki)

---
Last Updated: November 2024
