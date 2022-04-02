#!/usr/bin/env bash
# 静态客流分配
spark-submit --class flow.FlowWriter \
--master yarn --deploy-mode cluster \
--driver-memory 512M --executor-cores 2 \
--num-executors 1 \
--jars $(echo ../lib/*.jar | tr ' ' ',') \
$(dirname $(pwd))/lib/data-distribution-1.0-SNAPSHOT.jar