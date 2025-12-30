import csv
import time
import random
import json
import os
from kafka import KafkaProducer

SERVER_IP = '100.109.42.7'
CHECKPOINT_FILE = 'checkpoint.txt'
# CẤU HÌNH KAFKA 
KAFKA_TOPIC = 'transactions'
KAFKA_BOOTSTRAP_SERVERS = [f'{SERVER_IP}:9092']
# 1. Đọc toàn bộ file vào RAM
data_in_ram = []
file_path = 'User0_credit_card_transactions.csv'

with open(file_path, 'r', encoding='utf-8') as f:
    reader = csv.DictReader(f)
    data_in_ram = list(reader)
    
print(f"Đã load xong {len(data_in_ram)} dòng vào RAM.")

# --- KIỂM TRA CHECKPOINT ---
start_index = 0
if os.path.exists(CHECKPOINT_FILE):
    with open(CHECKPOINT_FILE, 'r') as f:
        try:
            content = f.read().strip()
            if content:
                start_index = int(content) + 1 # Bắt đầu từ dòng tiếp theo
                print(f"-> Tìm thấy checkpoint. Sẽ tiếp tục từ dòng index: {start_index}")
        except ValueError:
            print("-> File checkpoint lỗi, sẽ chạy từ đầu.")
else:
    print("-> Chạy lần đầu tiên (từ dòng 0).")

# 2. Khởi tạo Producer
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

# 3. Gửi từ RAM (Sử dụng enumerate để lấy index)
try:
    for i, row in enumerate(data_in_ram):
        # Nếu dòng này đã gửi rồi (index nhỏ hơn start_index) thì bỏ qua
        if i < start_index:
            continue

        # Gửi data
        producer.send(KAFKA_TOPIC, value=row)
        print(f"[{i}] Đã gửi: {row}")
        
        # --- LƯU CHECKPOINT ---
        if i % 10 == 0:
            with open(CHECKPOINT_FILE, 'w') as f_chk:
                f_chk.write(str(i))

        # Sleep ngẫu nhiên 1-5s
        sleep_time = random.uniform(1, 5)
        time.sleep(sleep_time)

except KeyboardInterrupt:
    print("\nĐã dừng thủ công! Lần sau chạy lại sẽ tiếp tục từ dòng vừa dừng.")

producer.close()