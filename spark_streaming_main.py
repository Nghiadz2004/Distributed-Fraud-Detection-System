import sys
import json
import threading
from time import sleep

# Import PySpark & Kafka dependencies
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, concat, lit, when, udf, regexp_replace, abs
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
from kafka import KafkaConsumer 

# ---------------------------------------------------------
# CẤU HÌNH HỆ THỐNG
# ---------------------------------------------------------
KAFKA_SERVER = "100.109.42.7:9092" # Địa chỉ Kafka
HDFS_NAMENODE_IP = "100.65.123.127" # Địa chỉ NameNode HDFS

# Đường dẫn lưu dữ liệu trên HDFS
HDFS_PATH = f"hdfs://{HDFS_NAMENODE_IP}:9000/user/project/processed_data/" 
CHECKPOINT_PATH = f"hdfs://{HDFS_NAMENODE_IP}:9000/user/project/checkpoint/"

# Biến toàn cục lưu tỉ giá hiện tại (Mặc định 25000 nếu chưa có data)
CURRENT_USD_RATE = 25000.0 

# ---------------------------------------------------------
# MODULE ĐỌC TỈ GIÁ (THREAD RIÊNG) 
# ---------------------------------------------------------
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
# MAIN SPARK STREAMING PIPELINE
# ---------------------------------------------------------
def main():
    # Khởi tạo Spark Session
    spark = SparkSession.builder \
        .appName("CreditCardFraudDetection") \
        .config("spark.streaming.stopGracefullyOnShutdown", "true") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    # Định nghĩa Schema
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

    # Parse JSON
    json_df = kafka_stream.select(
        from_json(col("value").cast("string"), schema).alias("data")
    ).select("data.*")

    # ---------------------------------------------------------
    # XỬ LÝ LOGIC NGHIỆP VỤ
    # ---------------------------------------------------------    
    processed_df = json_df \
        .withColumn("Amount_Clean", 
                    abs(regexp_replace(col("Amount"), "\\$", "").cast("double"))) \
        .withColumn("Transaction_Date", 
                    concat(col("Day"), lit("/"), col("Month"), lit("/"), col("Year"))) \
        .withColumn("Transaction_Time", col("Time")) \
        .withColumn("Is_Fraud_Flag", 
                    when(col("Is Fraud?") == "Yes", 1).otherwise(0)) \
        .select(
            col("User"),
            col("Card"),
            col("Transaction_Date"),
            col("Transaction_Time"),
            col("Year"), col("Month"), col("Day"),
            col("Merchant Name"),
            col("Merchant City"),
            col("Merchant State"),
            col("Zip"),
            col("Amount_Clean").alias("Amount_USD"),
            col("Is Fraud?"),
            col("Is_Fraud_Flag")
        )

    # ---------------------------------------------------------
    # HÀM GHI DỮ LIỆU
    # ---------------------------------------------------------
    def process_batch_data(df, epoch_id):
        if df.rdd.isEmpty():
            return

        current_rate_val = CURRENT_USD_RATE
        
        # Tính tiền VNĐ
        final_df = df.withColumn("Exchange_Rate", lit(current_rate_val)) \
                     .withColumn("Amount_VND", col("Amount_USD") * lit(current_rate_val))

        print(f"--- Batch ID: {epoch_id} | Rate: {current_rate_val} ---")
        final_df.show(5, truncate=False)

        # Tạo cột phụ để dùng cho Partition
        final_df_with_partition = final_df \
            .withColumn("p_Year", col("Year")) \
            .withColumn("p_Month", col("Month")) \
            .withColumn("p_Day", col("Day"))

        # Ghi file CSV
        final_df_with_partition.coalesce(1) \
            .write \
            .mode("append") \
            .partitionBy("p_Year", "p_Month", "p_Day") \
            .option("header", "true") \
            .option("sep", ",") \
            .csv(HDFS_PATH)

    # Kích hoạt Streaming
    query = processed_df.writeStream \
        .foreachBatch(process_batch_data) \
        .outputMode("append") \
        .option("checkpointLocation", CHECKPOINT_PATH) \
        .trigger(processingTime="10 seconds") \
        .start()
        
    query.awaitTermination()

if __name__ == "__main__":
    main()