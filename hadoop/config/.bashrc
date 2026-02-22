# Khai báo thư mục cài đặt Java (Cần thiết cho JVM)
export JAVA_HOME=/opt/jdk-17.0.10+7

# Khai báo thư mục gốc của Hadoop
export HADOOP_HOME=/opt/hadoop
export HADOOP_INSTALL=$HADOOP_HOME

# Khai báo đường dẫn cho các phân hệ của Hadoop (HDFS, YARN, MapReduce)
export HADOOP_MAPRED_HOME=$HADOOP_HOME
export HADOOP_COMMON_HOME=$HADOOP_HOME
export HADOOP_HDFS_HOME=$HADOOP_HOME
export YARN_HOME=$HADOOP_HOME

# Cấu hình thư viện Native (tối ưu hiệu năng nén/ghi dữ liệu bằng C++)
export HADOOP_COMMON_LIB_NATIVE_DIR=$HADOOP_HOME/lib/native
export HADOOP_OPTS="-Djava.library.path=$HADOOP_HOME/lib/native"

# Khai báo thư mục cài đặt Apache Spark
export SPARK_HOME=/opt/spark

# Giúp Spark tìm thấy cấu hình của Hadoop/YARN để chạy mode Cluster
export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop

# Thêm tất cả các thư mục chứa lệnh thực thi vào PATH hệ thống
# Giúp gõ các lệnh hdfs, yarn, spark-submit... từ bất cứ đâu
export PATH=$PATH:$HADOOP_HOME/sbin:$HADOOP_HOME/bin:$JAVA_HOME/bin:$SPARK_HOME/bin:$SPARK_HOME/sbin
