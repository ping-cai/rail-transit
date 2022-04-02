#!/usr/bin/env bash
spark-submit --class section.SectionMigration \
--master yarn --deploy-mode cluster \
--driver-memory 512M --executor-cores 2 \
--num-executors 1 \
--jars $(echo ../lib/*.jar | tr ' ' ',') \
$(dirname $(pwd))/lib/data-migration-1.0-SNAPSHOT.jar