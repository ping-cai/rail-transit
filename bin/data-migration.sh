#!/usr/bin/env bash
spark-submit --class section.SectionMigration \
--master yarn \
--deploy-mode cluster \
--driver-memory 1G \
--executor-cores 2 \
--executor-memory 2G \
--num-executors 1 \
--jars $(echo ../lib/*.jar | tr ' ' ',') \
$(dirname $(pwd))/lib/data-migration-1.0-SNAPSHOT.jar