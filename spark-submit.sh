#!/bin/bash
set -ex
export PATH="/usr/local/sbt/bin:$PATH"
cd /data
gunzip -d 2015_07_22_mktplace_shop_web_log_sample.log.gz
cd /
sbt package
/spark/bin/spark-submit \
--class ${SPARK_APPLICATION_MAIN_CLASS} \
--master ${SPARK_MASTER} \
 ${SPARK_SUBMIT_ARGS} \
 ${SPARK_APPLICATION_JAR_LOCATION} \
 ${SPARK_APPLICATION_ARGS} \
