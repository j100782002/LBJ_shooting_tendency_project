import json
from google.cloud import bigquery
from google.oauth2 import service_account

def process_and_load_to_bigquery(input_json_file, dataset_id, table_id, key_path):
    """
    处理数据并在 BigQuery 中创建表格并插入数据。

    :param input_json_file: 输入的 JSON 文件路径
    :param dataset_id: BigQuery 数据集 ID
    :param table_id: BigQuery 表格 ID
    :param key_path: 服务账号密钥文件路径
    """
    # 加载服务账号密钥文件
    credentials = service_account.Credentials.from_service_account_file(key_path)
    
    # 创建一个 BigQuery 客户端
    client = bigquery.Client(credentials=credentials, project=credentials.project_id)
    
    # 读取 JSON 文件
    with open(input_json_file, 'r') as f:
        data = json.load(f)
    
    # 清洗数据（这里假设对数据进行简单的清洗）
    processed_data = []
    for item in data:
        processed_item = {key: value for key, value in item.items()}
        processed_data.append(processed_item)
    
    # 动态生成表格 schema
    if processed_data:
        first_item = processed_data[0]
        schema = [bigquery.SchemaField(key, "STRING") for key in first_item.keys()]
    else:
        print("No data to insert")
        return
    
    # 创建 dataset（如果不存在）
    dataset_ref = client.dataset(dataset_id)
    dataset = bigquery.Dataset(dataset_ref)
    dataset = client.create_dataset(dataset, exists_ok=True)
    print(f"Dataset {dataset_id} created or already exists.")
    
    # 创建表格引用
    table_ref = dataset_ref.table(table_id)
    
    # 创建表格
    table = bigquery.Table(table_ref, schema=schema)
    table = client.create_table(table, exists_ok=True)  # 如果表格已经存在，则不创建
    print(f"Table {table_id} created or already exists in dataset {dataset_id}.")
    
    # 将数据插入到表格中
    errors = client.insert_rows_json(table, processed_data)
    if not errors:
        print(f"New rows have been added to {table_id}.")
    else:
        print(f"Encountered errors while inserting rows: {errors}")
