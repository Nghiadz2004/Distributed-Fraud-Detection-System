#!/bin/bash
# Script to reset the state of the Distributed Fraud Detection System
rm {checkpoint_*,part_*.csv}

kafka-topics.sh --bootstrap-server $KAFKA_BROKER --delete --topic transactions
kafka-topics.sh --bootstrap-server $KAFKA_BROKER --delete --topic exchange_rates

kafka-topics.sh --list --bootstrap-server $KAFKA_BROKER