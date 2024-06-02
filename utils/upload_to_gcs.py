from google.cloud import storage
from google.oauth2 import service_account

def upload_to_gcs(local_file_path, bucket_name, blob_name, key_path):
    # 加载服务账号密钥文件
    credentials = service_account.Credentials.from_service_account_file(key_path)
    
    # 创建一个GCS客户端
    client = storage.Client(credentials=credentials)
    
    # 获取GCS桶
    bucket = client.get_bucket(bucket_name)
    
    # 创建一个Blob对象
    blob = bucket.blob(blob_name)
    
    # 上传文件
    blob.upload_from_filename(local_file_path)
    print(f"File {local_file_path} uploaded to {blob_name} in bucket {bucket_name}.")
