#!/usr/bin/env bash
#od数据配对脚本+聚合脚本
spark-submit \
--class AfcPair \
--master yarn \
--deploy-mode cluster \
--driver-memory 1G \
--executor-cores 2 \
--executor-memory 2G \
--num-executors 1 \
--jars $(echo ../lib/*.jar | tr ' ' ',') $(dirname $(pwd))/lib/data-preparation-1.0-SNAPSHOT.jar \
$1