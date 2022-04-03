#!/usr/bin/env bash
# mysql导入到hdfs的脚本
spark-submit \
--class jdbc.MysqlToHDFS \
--master yarn \
--deploy-mode cluster \
--driver-memory 1G \
--executor-cores 2 \
--executor-memory 2G \
--num-executors 1 \
--jars $(echo ../lib/*.jar | tr ' ' ',') $(dirname $(pwd))/lib/data-migration-1.0-SNAPSHOT.jar \
$1