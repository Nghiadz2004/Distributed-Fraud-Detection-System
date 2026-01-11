import csv
import time
import random
import json
import os
import multiprocessing  # <--- Thư viện quan trọng để chạy song song
from kafka import KafkaProducer

# --- CẤU HÌNH CHUNG ---
SERVER_IP = '100.109.42.7'
KAFKA_TOPIC = 'transactions'
KAFKA_BOOTSTRAP_SERVERS = [f'{SERVER_IP}:9092']
ORIGINAL_FILE = 'data/User0_credit_card_transactions.csv'
NUM_WORKERS = 3
GLOBAL_CHECKPOINT_FILE = 'checkpoint.txt'

# ---------------------------------------------------------
# HÀM 1: TỰ ĐỘNG CHIA FILE (SPLITTER)
# ---------------------------------------------------------
def split_csv_file():
    if os.path.exists("part_1.csv"):
        print("-> Các file con đã tồn tại. Bỏ qua bước chia file.")
        return [f"part_{i+1}.csv" for i in range(NUM_WORKERS)]

    print(f"-> Đang chia file '{ORIGINAL_FILE}' thành {NUM_WORKERS} phần...")

    # --- KIỂM TRA CHECKPOINT ---
    start_index = 0
    if os.path.exists(GLOBAL_CHECKPOINT_FILE):
        with open(GLOBAL_CHECKPOINT_FILE, 'r') as f:
            try:
                content = f.read().strip()
                if content:
                    start_index = int(content) + 1  # Bắt đầu từ dòng tiếp theo
                    print(f"-> Tìm thấy checkpoint. Sẽ tiếp tục từ dòng index: {start_index}")
            except ValueError:
                print("-> File checkpoint lỗi, sẽ chạy từ đầu.")
    else:
        print("-> Chạy lần đầu tiên (từ dòng 0).")

    # Đếm dòng
    with open(ORIGINAL_FILE, 'r', encoding='utf-8') as f:
        header = f.readline()
        total_lines = sum(1 for _ in f)

    lines_per_part = (total_lines - start_index) // NUM_WORKERS + 1
    generated_files = []

    with open(ORIGINAL_FILE, 'r', encoding='utf-8') as f_in:
        header = f_in.readline()
        file_idx = 1
        line_count = 0
        current_out = None
        offset = 0

        for line in f_in:
            if offset < start_index:
                offset += 1
                continue

            if current_out is None or line_count >= lines_per_part:
                if current_out:
                    current_out.close()
                out_name = f"part_{file_idx}.csv"
                generated_files.append(out_name)
                current_out = open(out_name, 'w', encoding='utf-8')
                current_out.write(header)
                file_idx += 1
                line_count = 0
            current_out.write(line)
            line_count += 1
        if current_out:
            current_out.close()

    print("-> Chia file hoàn tất!")
    return generated_files

# ---------------------------------------------------------
# HÀM 2: LOGIC CỦA MỖI PRODUCER (WORKER)
# ---------------------------------------------------------
def run_producer_worker(file_path, worker_id):
    """
    Hàm này sẽ chạy độc lập trong một Process riêng biệt
    """
    pid = os.getpid()  # Lấy ID của tiến trình trong hệ điều hành
    checkpoint_file = f"checkpoint_{file_path}.txt"
    print(f"--- [Worker {worker_id} - PID {pid}] Bắt đầu xử lý file: {file_path} ---")

    # 1. Load data riêng của worker này
    data_in_ram = []
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            data_in_ram = list(reader)
    except Exception as e:
        print(f"[Worker {worker_id}] Lỗi đọc file: {e}")
        return

    # 2. Checkpoint check
    start_index = 0
    if os.path.exists(checkpoint_file):
        with open(checkpoint_file, 'r') as f:
            try:
                start_index = int(f.read().strip()) + 1
                print(f"[Worker {worker_id}] Tiếp tục từ dòng {start_index}")
            except:
                pass

    # 3. Khởi tạo Kafka Producer (QUAN TRỌNG: Phải tạo bên trong hàm worker)
    # Không được tạo ở ngoài rồi truyền vào vì Producer không thread-safe khi qua process
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda x: json.dumps(x).encode('utf-8'),
        linger_ms=10, batch_size=16384  # Tối ưu tốc độ
    )

    # 4. Vòng lặp gửi tin
    try:
        count = 0
        for i, row in enumerate(data_in_ram):
            if i < start_index:
                continue

            # Gửi dữ liệu (Kafka tự chia partition)
            producer.send(KAFKA_TOPIC, value=row)
            count += 1

            # Lưu checkpoint & Log mỗi 50 dòng
            if i % 50 == 0:
                with open(checkpoint_file, 'w') as f_chk:
                    f_chk.write(str(i))
                # Print ít thôi kẻo loạn màn hình
                if i % 200 == 0:
                    print(f"[Worker {worker_id}] Đã gửi dòng {i}...")

            # Sleep ngẫu nhiên để giả lập
            time.sleep(random.uniform(1, 5))

    except KeyboardInterrupt:
        print(f"[Worker {worker_id}] Dừng.")
    finally:
        producer.close()
        print(f"[Worker {worker_id}] Hoàn thành công việc!")

# ---------------------------------------------------------
# HÀM MAIN: ĐIỀU PHỐI CHUNG
# ---------------------------------------------------------
if __name__ == "__main__":
    # Bước 1: Chia file
    list_files = split_csv_file()

    # Bước 2: Tạo và khởi chạy các Process
    processes = []
    print("\n>>> KÍCH HOẠT HỆ THỐNG ĐA TIẾN TRÌNH (MULTIPROCESSING) <<<")

    for idx, file_name in enumerate(list_files):
        # Tạo tiến trình
        p = multiprocessing.Process(target=run_producer_worker, args=(file_name, idx+1))
        processes.append(p)
        p.start()  # Bắt đầu chạy ngay lập tức

    # Bước 3: Chờ các con chạy (Main sẽ không thoát cho đến khi các con xong)
    try:
        for p in processes:
            p.join()
    except KeyboardInterrupt:
        print("\n!!! Stop producer system!!!")
        for p in processes:
            p.terminate()  # Terminate subprocess when user presses Ctrl + C
