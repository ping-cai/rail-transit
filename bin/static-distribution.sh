#!/usr/bin/env bash
# 静态客流分配
spark-submit \
--class flow.FlowWriter \
--master yarn \
--deploy-mode cluster \
--driver-memory 1G \
--executor-cores 2 \
--executor-memory 2G \
--num-executors 1 \
--jars $(echo ../lib/*.jar | tr ' ' ',') \
$(dirname $(pwd))/lib/data-distribution-1.0-SNAPSHOT.jar