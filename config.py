import os
import sys

# --- HÀM LẤY BIẾN MÔI TRƯỜNG ---
def get_env_variable(var_name, default_value=None):
    value = os.getenv(var_name)
    if value is None:
        if default_value is None:
            # Nếu không tìm thấy biến và không có mặc định -> Báo lỗi và dừng chương trình
            print(f"LỖI: Không tìm thấy biến môi trường '{var_name}' trong .bashrc!")
            print("Vui lòng chạy lệnh: source ~/.bashrc hoặc export biến này.")
            sys.exit(1)
        return default_value
    return value

# --- CẤU HÌNH TỰ ĐỘNG ---

# 1. Đọc chuỗi "IP:PORT" từ biến KAFKA_BROKER (ví dụ: 100.109.42.7:9092)
# Nếu quên set, nó sẽ lấy mặc định là 'localhost:9092' để không bị crash
BROKER_ADDRESS = get_env_variable('KAFKA_BROKER', 'localhost:9092')

# 2. Tách lấy riêng IP (nếu code logic của bạn cần dùng riêng IP)
# Logic: Lấy phần tử đầu tiên trước dấu ":"
SERVER_IP = BROKER_ADDRESS.split(':')[0]

# 3. Cấu hình cho Kafka Producer (Kafka yêu cầu dạng list)
KAFKA_BOOTSTRAP_SERVERS = [BROKER_ADDRESS]

# --- CÁC CẤU HÌNH KHÁC ---
KAFKA_TRANSACTIONS = 'transactions'
KAFKA_EXCHANGE_RATES = 'exchange_rates'
ORIGINAL_FILE = 'data/User0_credit_card_transactions.csv'
NUM_WORKERS = 3
GLOBAL_CHECKPOINT_FILE = 'checkpoint.txt'

# --- IN RA ĐỂ KIỂM TRA (DEBUG) ---
print(f"--> Cấu hình đã load: IP={SERVER_IP} | Broker={KAFKA_BOOTSTRAP_SERVERS}")
