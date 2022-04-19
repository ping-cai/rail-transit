#!/usr/bin/env bash
# 导出hdfs数据到MySQL的脚本
spark-submit \
--class flow.FlowMigration \
--master yarn \
--deploy-mode cluster \
--driver-memory 1G \
--executor-cores 2 \
--executor-memory 2G \
--num-executors 1 \
--jars $(echo ../lib/*.jar | tr ' ' ',') \
$(dirname $(pwd))/lib/data-distribution-1.0-SNAPSHOT.jar \
$1