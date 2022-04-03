#!/usr/bin/env bash
# 抽取afc数据部分字段脚本
spark-submit \
--class AfcExtract \
--master yarn \
--deploy-mode cluster \
--driver-memory 1G \
--executor-cores 2 \
--executor-memory 2G \
--num-executors 1 \
--jars $(echo ../lib/*.jar | tr ' ' ',') $(dirname $(pwd))/lib/data-preparation-1.0-SNAPSHOT.jar \
$1