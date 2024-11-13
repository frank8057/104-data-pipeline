# 使用官方的 Airflow 镜像作为基础镜像
FROM apache/airflow:2.10.2-python3.8

# 设置 UID 和 GID
ARG AIRFLOW_UID=1005
ARG AIRFLOW_GID=0  # GID 设置为 0，使用 root 组

USER root

# 设置 airflow 用户的 UID 和 GID，使用现有的 root 组 (GID 0)
RUN usermod -u ${AIRFLOW_UID} -g ${AIRFLOW_GID} airflow

# 安装必要的依赖项
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    wget \
    gnupg2 \
    unzip \
    curl \
    libmariadb-dev \
    git \
    && curl -sSL https://dl.google.com/linux/linux_signing_key.pub | gpg --dearmor -o /usr/share/keyrings/google-linux-signing-key.gpg \
    && echo "deb [arch=amd64 signed-by=/usr/share/keyrings/google-linux-signing-key.gpg] http://dl.google.com/linux/chrome/deb/ stable main" > /etc/apt/sources.list.d/google-chrome.list \
    && apt-get update \
    && apt-get install -y --no-install-recommends google-chrome-stable \
    && rm -rf /var/lib/apt/lists/*

# 创建所有必要目录并设置权限
RUN mkdir -p /opt/airflow/{dags,logs,plugins,config,jobs_csv,data,resources,tasks,dbt_project/logs,.dbt,keys,dbt_project/dbt_packages,dbt_project/target} \
    && chown -R airflow:root /opt/airflow \
    && chmod -R 755 /opt/airflow \
    && chmod -R 777 /opt/airflow/jobs_csv \
    && chmod -R 777 /opt/airflow/data \
    && chmod -R 777 /opt/airflow/resources \
    && chmod -R 777 /opt/airflow/dbt_project \
    && chmod -R 777 /opt/airflow/dbt_project/logs \
    && chmod -R 777 /opt/airflow/dbt_project/dbt_packages \
    && chmod -R 777 /opt/airflow/dbt_project/target \
    && chmod -R 777 /opt/airflow/.dbt \
    && chmod -R 777 /opt/airflow/keys

# 确保 Python 路径正确
ENV PYTHONPATH="/opt/airflow:/opt/airflow/tasks:/opt/airflow/dags:${PYTHONPATH}"

# 复制文件并设置正确的权限
COPY --chown=airflow:root requirements.txt /opt/airflow/
COPY --chown=airflow:root tasks/ /opt/airflow/tasks/
COPY --chown=airflow:root dags/ /opt/airflow/dags/

# 确保 tasks 目录中的所有文件都有正确的权限
RUN chown -R airflow:root /opt/airflow/tasks \
    && chmod -R 755 /opt/airflow/tasks \
    && touch /opt/airflow/tasks/__init__.py \
    && chown airflow:root /opt/airflow/tasks/__init__.py \
    && chmod 644 /opt/airflow/tasks/__init__.py

# 新增 dbt 的日誌文件並設置權限
RUN touch /opt/airflow/dbt_project/logs/dbt.log \
    && chown airflow:root /opt/airflow/dbt_project/logs/dbt.log \
    && chmod 777 /opt/airflow/dbt_project/logs/dbt.log

# 切換到 airflow 用戶以進行後續操作
USER airflow

# 安装 Python 依赖
RUN pip install --no-cache-dir -r /opt/airflow/requirements.txt \
    && pip install dbt-bigquery
