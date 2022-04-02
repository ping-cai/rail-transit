#!/usr/bin/env bash
# mysql导入到hdfs的脚本
spark-submit \
--class jdbc.MysqlToHDFS \
--master yarn \
--deploy-mode cluster \
--driver-memory 512M \
--executor-cores 2 \
--num-executors 1 \
--jars $(echo ../lib/*.jar | tr ' ' ',') $(dirname $(pwd))/lib/data-migration-1.0-SNAPSHOT.jar \
$1