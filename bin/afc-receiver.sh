#!/usr/bin/env bash
# 实时接受kafka数据并存储到hdfs上
spark-submit \
--class afc.AfcReceiver \
--master yarn \
--deploy-mode cluster \
--driver-memory 512M \
--executor-cores 1 \
--executor-memory 512M \
--num-executors 2 \
--jars $(echo ../lib/*.jar | tr ' ' ',') $(dirname $(pwd))/lib/data-migration-1.0-SNAPSHOT.jar