#!/bin/bash
# Script to create Kafka topics for the Distributed Fraud Detection System
kafka-topics.sh --bootstrap-server $KAFKA_BROKER --create \
    --topic exchange_rates \
    --partitions 1 \
    --replication-factor 1 \
    --config cleanup.policy=compact \
    --config segment.ms=3600000

kafka-topics.sh --bootstrap-server $KAFKA_BROKER --create \
  --topic transactions \
  --partitions 3 \
  --replication-factor 1 \
  --config min.insync.replicas=1 \
  --config segment.bytes=1073741824

kafka-topics.sh --bootstrap-server $KAFKA_BROKER --describe --topic transactions
kafka-topics.sh --bootstrap-server $KAFKA_BROKER --describe --topic exchange_rates