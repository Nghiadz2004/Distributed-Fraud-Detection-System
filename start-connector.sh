#!/usr/bin/env bash
# Script to start Spark Thrift Server with configuration on YARN
start-thriftserver.sh \
--master yarn \
--deploy-mode client \
--executor-memory 1G \
--executor-cores 1 \
--conf spark.yarn.am.memory=512m \
--conf spark.yarn.am.nodeLabelExpression="master_only" \
--conf spark.yarn.executor.nodeLabelExpression="master_only" \
--conf spark.sql.hive.thriftServer.singleSession=true \
--conf spark.yarn.appMasterEnv.JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64 \
--conf spark.executorEnv.JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64 \
--hiveconf hive.server2.thrift.bind.host=100.65.123.127 \
--hiveconf hive.server2.thrift.port=10000 \
--hiveconf hive.server2.authentication=NONE

