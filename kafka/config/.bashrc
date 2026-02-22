# Chỉ định Java 17 cho Spark/Kafka
export JAVA_HOME=/opt/jdk-17.0.10+7

# Đường dẫn đến thư mục gốc của Kafka
export KAFKA_HOME=/opt/kafka
# Địa chỉ IP và Port của Kafka Broker để các ứng dụng kết nối vào
export KAFKA_BROKER=100.99.156.93:9092

# Cập nhật biến PATH để có thể chạy các lệnh 'java' hoặc 'kafka-topics' 
# trực tiếp từ bất kỳ đâu trong terminal
export PATH=$JAVA_HOME/bin:$PATH:$KAFKA_HOME/bin