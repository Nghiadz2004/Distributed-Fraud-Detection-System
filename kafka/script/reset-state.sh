#!/bin/bash
# Script để xóa Kafka topics và các file checkpoint, part CSV, reset trạng thái được sử dụng để phát sinh dữ liệu giả lập
rm {checkpoint_*,part_*.csv}

kafka-topics.sh --bootstrap-server $KAFKA_BROKER --delete --topic transactions
kafka-topics.sh --bootstrap-server $KAFKA_BROKER --delete --topic exchange_rates

kafka-topics.sh --list --bootstrap-server $KAFKA_BROKER