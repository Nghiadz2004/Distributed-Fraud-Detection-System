#!/bin/bash
# Script để tạo Kafka topics

# Cấu hình Topic Exchange rates:
# - partitions 1: Chỉ tạo 1 phân vùng (vì dữ liệu tỷ giá thường ít và cần thứ tự chuẩn)
# - replication-factor 1: số bản sao lưu là 1 (chế độ chạy đơn lẻ)
# - cleanup.policy=compact: chế độ "nén" dữ liệu. Kafka sẽ chỉ giữ lại giá trị MỚI NHẤT cho mỗi Key, 
# xóa các giá trị cũ của cùng Key đó để tiết kiệm bộ nhớ.
# - segment.ms=3600000: Sau 1 giờ (3,600,000ms), Kafka sẽ đóng file segment hiện tại để chuẩn bị cho việc nén (compact).  
kafka-topics.sh --bootstrap-server $KAFKA_BROKER --create \
    --topic exchange_rates \
    --partitions 1 \
    --replication-factor 1 \
    --config cleanup.policy=compact \
    --config segment.ms=3600000


# Cấu hình Topic Transactions:
# - 3 partitions: Tăng throughput, cho phép 3 Consumer đọc song song.
# - min.insync.replicas=1: Số lượng bản sao tối thiểu phải xác nhận đã ghi dữ liệu thành công trước khi báo cho Producer.
# - segment.bytes=1GB: Kích thước tối đa của mỗi file segment là 1GB. Khi đạt mức này, Kafka sẽ tạo file mới.
kafka-topics.sh --bootstrap-server $KAFKA_BROKER --create \
  --topic transactions \
  --partitions 3 \
  --replication-factor 1 \
  --config min.insync.replicas=1 \
  --config segment.bytes=1073741824


# Hiển thị thông tin chi tiết của các topic vừa tạo
kafka-topics.sh --bootstrap-server $KAFKA_BROKER --describe --topic transactions
kafka-topics.sh --bootstrap-server $KAFKA_BROKER --describe --topic exchange_rates