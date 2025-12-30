import sys
import json
import threading
from time import sleep

# Import PySpark & Kafka dependencies
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, concat, lit, when, udf, regexp_replace
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
from kafka import KafkaConsumer # Dùng để đọc rate riêng biệt

# ---------------------------------------------------------
# CẤU HÌNH HỆ THỐNG
# ---------------------------------------------------------
KAFKA_SERVER = "100.109.42.7:9092" # Địa chỉ Kafka
HDFS_PATH = "hdfs://localhost:9000/user/project/processed_data/" # Đường dẫn HDFS 
CHECKPOINT_PATH = "hdfs://localhost:9000/user/project/checkpoint/"

# Biến toàn cục lưu tỉ giá hiện tại (Mặc định 25000 nếu chưa có data)
CURRENT_USD_RATE = 25000.0 

# ---------------------------------------------------------
# 1. MODULE ĐỌC TỈ GIÁ (THREAD RIÊNG) 
# ---------------------------------------------------------
# Hàm này chạy ngầm để liên tục cập nhật tỉ giá từ topic 'exchange_rates'
# Giúp luồng xử lý chính không bị chặn (blocking).
def fetch_exchange_rate():
    global CURRENT_USD_RATE
    consumer = KafkaConsumer(
        'exchange_rates',
        bootstrap_servers=[KAFKA_SERVER],
        auto_offset_reset='earliest',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )
    print(">>> Đang lắng nghe topic exchange_rates...")
    for message in consumer:
        try:
            data = message.value
            # Giả sử format message: {"currency": "USD", "rate": 25450, "timestamp": ...}
            new_rate = float(data.get('rate', 25000))
            if new_rate > 0:
                CURRENT_USD_RATE = new_rate
                print(f">>> Cập nhật tỉ giá mới: {CURRENT_USD_RATE} VND")
        except Exception as e:
            print(f"Lỗi đọc tỉ giá: {e}")

# Khởi động thread đọc tỉ giá
rate_thread = threading.Thread(target=fetch_exchange_rate)
rate_thread.daemon = True
rate_thread.start()

# ---------------------------------------------------------
# 2. MAIN SPARK STREAMING PIPELINE [cite: 112]
# ---------------------------------------------------------
def main():
    # Khởi tạo Spark Session
    spark = SparkSession.builder \
        .appName("CreditCardFraudDetection") \
        .config("spark.streaming.stopGracefullyOnShutdown", "true") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    # Định nghĩa Schema cho dữ liệu JSON từ Transactions
    schema = StructType([
        StructField("User", StringType(), True),
        StructField("Card", StringType(), True),
        StructField("Year", StringType(), True),
        StructField("Month", StringType(), True),
        StructField("Day", StringType(), True),
        StructField("Time", StringType(), True),
        StructField("Amount", StringType(), True),
        StructField("Use Chip", StringType(), True),
        StructField("Merchant Name", StringType(), True),
        StructField("Merchant City", StringType(), True),
        StructField("Merchant State", StringType(), True),
        StructField("Zip", StringType(), True),
        StructField("MCC", StringType(), True),
        StructField("Errors?", StringType(), True),
        StructField("Is Fraud?", StringType(), True)
    ])

    # Đọc Stream từ Kafka
    kafka_stream = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_SERVER) \
        .option("subscribe", "transactions") \
        .option("startingOffsets", "latest") \
        .load()

    # Chuyển đổi dữ liệu từ Binary (Kafka) sang String rồi Parse JSON
    json_df = kafka_stream.select(
        from_json(col("value").cast("string"), schema).alias("data")
    ).select("data.*")

    # ---------------------------------------------------------
    # 3. XỬ LÝ LOGIC NGHIỆP VỤ
    # ---------------------------------------------------------
    
    # Lọc bỏ giao dịch gian lận (Fraud) 
    # Chỉ giữ lại dòng có Is Fraud? là 'No'
    clean_df = json_df.filter(col("Is Fraud?") == "No")

    # Xử lý định dạng Thời gian & Tiền tệ 
    # Amount trong CSV thường có dấu $ (ví dụ $54.20), cần replace và ép kiểu float
    processed_df = clean_df \
        .withColumn("Amount_Clean", 
                    regexp_replace(col("Amount"), "\\$", "").cast("double")) \
        .withColumn("Transaction_Date", 
                    concat(col("Day"), lit("/"), col("Month"), lit("/"), col("Year"))) \
        .withColumn("Transaction_Time", col("Time")) \
        .select(
            col("User"),
            col("Card"),
            col("Transaction_Date"),
            col("Transaction_Time"),
            col("Merchant Name"),
            col("Merchant City"),
            col("Amount_Clean").alias("Amount_USD")
        )

    # ---------------------------------------------------------
    # 4. HÀM GHI DỮ LIỆU (FOREACHBATCH) 
    # ---------------------------------------------------------
    # Hàm này sẽ được gọi mỗi khi có một batch dữ liệu mới
    def process_batch_data(df, epoch_id):
        # Lấy tỉ giá hiện tại từ biến toàn cục (đã được Thread cập nhật)
        current_rate_val = CURRENT_USD_RATE
        
        # Bước 3.3: Tính toán tiền VNĐ 
        # Sử dụng lit() để nhân với giá trị tỉ giá tại thời điểm batch chạy
        final_df = df.withColumn("Exchange_Rate", lit(current_rate_val)) \
                     .withColumn("Amount_VND", col("Amount_USD") * lit(current_rate_val))

        # Hiển thị console để debug
        print(f"--- Batch ID: {epoch_id} | Rate: {current_rate_val} ---")
        final_df.show(5, truncate=False)

        # Ghi xuống HDFS (Mode Append) [cite: 121]
        # Lưu định dạng Parquet để tối ưu cho Power BI
        #final_df.write.mode("append").parquet(HDFS_PATH)

    # Kích hoạt Streaming
    query = processed_df.writeStream \
        .foreachBatch(process_batch_data) \
        .outputMode("append") \
        .trigger(processingTime="5 seconds") \
        .start()
        #.option("checkpointLocation", CHECKPOINT_PATH) \
        
        

    query.awaitTermination()

if __name__ == "__main__":
    main()