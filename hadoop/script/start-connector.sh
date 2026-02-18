#!/usr/bin/env bash
export JAVA_HOME=/opt/jdk-17.0.10+7

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
