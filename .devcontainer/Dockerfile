# 使用 Python 3.8 slim 作為基礎映像
FROM python:3.8-slim

# 設定工作目錄為 /app
WORKDIR /app

# 複製需求檔案 requirements.txt 到工作目錄
COPY requirements.txt .

# 安裝需求中的 Python 套件
RUN pip install --no-cache-dir -r requirements.txt

# 安裝 Apache Airflow
RUN pip install apache-airflow

# 複製應用程式代碼到工作目錄
COPY . .

# 設定容器啟動時運行的命令，啟動所有必要的服務
CMD ["sh", "-c", "airflow scheduler & airflow webserver"]
