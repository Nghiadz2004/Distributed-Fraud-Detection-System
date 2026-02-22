#!/usr/bin/env bash
export JAVA_HOME=/opt/jdk-17.0.10+7

# Tham số,Ý nghĩa
# --master yarn,Chạy trên cluster Hadoop YARN.
# --deploy-mode client,Chạy Driver tại máy cục bộ (bắt buộc cho Thrift Server).
# --driver-memory 1G,RAM cho bộ điều phối (Driver).
# --executor-memory 1500M,RAM cho mỗi đơn vị xử lý (Executor).
# --executor-cores 2,Số lõi CPU cho mỗi Executor.
# spark.dynamicAllocation.enabled=true,Tự động tăng/giảm số lượng Executor theo tải.
# spark.dynamicAllocation.minExecutors=0,Giải phóng sạch Executor về 0 khi rảnh.
# spark.dynamicAllocation.maxExecutors=10,Giới hạn tối đa 10 Executor để tránh chiếm tài nguyên cụm.
# hive.server2.thrift.bind.host=0.0.0.0,Mở cổng kết nối cho tất cả các IP bên ngoài.
# hive.server2.thrift.port=10000,Cổng dịch vụ (mặc định là 10000).
# hive.server2.authentication=NONE,Không yêu cầu mật khẩu khi kết nối.
$SPARK_HOME/sbin/start-thriftserver.sh \
--master yarn \
--deploy-mode client \
--driver-memory 1G \
--executor-memory 1500M \
--executor-cores 2 \
--conf spark.dynamicAllocation.enabled=true \
--conf spark.dynamicAllocation.minExecutors=0 \
--conf spark.dynamicAllocation.maxExecutors=10 \
--hiveconf hive.server2.thrift.bind.host=0.0.0.0 \
--hiveconf hive.server2.thrift.port=10000 \
--hiveconf hive.server2.authentication=NONE

# Cơ chế điều phối: Nhờ --master yarn, YARN ResourceManager sẽ tự động ưu tiên đẩy các Executor sang node có tài nguyên trống (Node mạnh hơn) để đáp ứng yêu cầu tính toán.
# Xử lý tải nặng: Tham số dynamicAllocation cho phép hệ thống tự động tăng scale lên tối đa 10 Executors (mỗi executor 1.5GB RAM, 2 Cores) tùy theo độ phức tạp của truy vấn.
# Lưu ý Driver: Do chạy ở --deploy-mode client, Driver (1GB RAM) vẫn cố định tại node chạy script.