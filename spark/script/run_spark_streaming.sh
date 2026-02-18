#!/bin/bash

# Kích hoạt môi trường ảo
source ~/pyspark-env/bin/activate

# Thông báo
echo ">>> Đã kích hoạt môi trường ảo!"
echo ">>> Đang khởi động Spark Streaming..."

# Chạy lệnh Spark
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5 spark_streaming_main.py