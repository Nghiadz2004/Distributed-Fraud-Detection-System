from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime, timedelta
import requests
import msal

# --- CẤU HÌNH THÔNG TIN ---
CLIENT_ID = "53aa9161-6f16-4d59-82a0-0bb3c553cf6d"
TENANT_ID = "40127cd4-45f3-49a3-b05d-315a43a9f033"
WORKSPACE_ID = "22c1eff3-46ca-4316-b5ce-7e0efc6bfab3" 
DATASET_ID = "4217b4f4-5fa9-4715-bab4-0ed8ff807acf"

default_args = {
    'owner': 'hadoop_admin',
    'start_date': datetime(2026, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# --- HÀM LẤY TOKEN DÙNG DANH NGHĨA SINH VIÊN ---
def get_access_token():
    creds = Variable.get("pbi_credentials", deserialize_json=True)
    user_name = creds.get("username")
    user_password = creds.get("password")

    authority = f"https://login.microsoftonline.com/{TENANT_ID}"
    # Scope này cho phép app hành động dưới danh nghĩa của bạn đối với Power BI
    scopes = ["https://analysis.windows.net/powerbi/api/Dataset.ReadWrite.All"]

    # Khởi tạo PublicClient (không dùng secret)
    app = msal.PublicClientApplication(CLIENT_ID, authority=authority)
    
    # Gọi API lấy token bằng Username/Password
    result = app.acquire_token_by_username_password(user_name, user_password, scopes=scopes)    

    if "access_token" in result:
        return result['access_token']
    else:
        error_msg = result.get('error_description', 'Unknown error')
        raise Exception(f"Xác thực thất bại: {error_msg}")

# --- HÀM TRIGGER POWER BI REFRESH ---
def trigger_powerbi_refresh():
    # 1. Gọi hàm lấy token mới
    access_token = get_access_token()
    
    # 2. Gửi yêu cầu Refresh Dataset
    refresh_url = f"https://api.powerbi.com/v1.0/myorg/datasets/{DATASET_ID}/refreshes"
    headers = {'Authorization': f'Bearer {access_token}'}
    
    response = requests.post(refresh_url, headers=headers)
    
    if response.status_code == 202:
        print("✅ Power BI refresh triggered successfully!")
    elif response.status_code == 404:
        print("❌ Lỗi 404: Không tìm thấy Dataset. Hãy kiểm tra WORKSPACE_ID và DATASET_ID.")
        raise Exception("Dataset not found")
    else:
        print(f"❌ Failed. Status: {response.status_code}, Error: {response.text}")
        raise Exception(f"Power BI Refresh Failed: {response.text}")

# --- ĐỊNH NGHĨA DAG ---
with DAG(
    'daily_fraud_analysis_pipeline',
    default_args=default_args,
    schedule_interval='*/10 * * * *',  # Chạy vào mỗi phút chia hết cho 10 (0, 10, 20,...)
    catchup=False,
    tags=['debug', 'fraud_project']
) as dag:

    update_metadata = BashOperator(
        task_id='update_metadata',
        bash_command=(
            'beeline -u jdbc:hive2://ming3993server:10000 '
            '-e "MSCK REPAIR TABLE odap.credit_card_data; '
            'REFRESH TABLE odap.credit_card_data; '
            'SELECT COUNT(*) FROM odap.credit_card_data; '
            'REFRESH TABLE odap.v_credit_card;"'
        )
    )

    refresh_powerbi = PythonOperator(
        task_id='refresh_powerbi',
        python_callable=trigger_powerbi_refresh
    )

    update_metadata >> refresh_powerbi