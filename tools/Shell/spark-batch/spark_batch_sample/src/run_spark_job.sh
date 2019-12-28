#!/usr/bin/env bash


function run_spark() {
	echo "run spark job"
    ${SPARK_HOME}/bin/spark-submit \
    --class cn.focusmedia.data.warehouse.SparkHDFSDriver \
    --driver-cores 2 \
    --driver-memory 2g \
    --num-executors 15 \
    --executor-memory 2g \
    --executor-cores 6 \
    --master yarn \
    --deploy-mode cluster \
    --queue report \
    --conf park.shuffle.file.buffer=2048 \
    --conf spark.reducer.maxSizeInFlight=96 \
    --conf spark.shuffle.io.maxRetries=8 \
    --conf spark.memory.fraction=0.5 \
    --conf spark.memory.storageFraction=0.4 \
    --conf spark.default.parallelism=200 \
    hdfs://dw-cluster/spark/spark_hdfs_project_jars/spark-hdfs-1.0-SNAPSHOT-jar-with-dependencies.jar \
    ${1}
}

function test_status() {
    status=$1
    if [ $1 -ne 0 ]; then
        echo "spark job run failed"
        exit 8
    else
        echo "spark job run successfully"
        exit 0
    fi
}

function test_file() {
    FILE_PATH=$1
    hadoop fs -test -e ${FILE_PATH}/*.tmp
    if_exist=$?
    if [ $if_exist -ne 0 ]; then
        echo "tmp file not exist, will run spark job"
        return 0
    else
        echo "tmp file is exist, will sleep 5 seconds"
        sleep 5s
        return 2
    fi
}

FILE_PATH=$1
hadoop fs -test -e ${FILE_PATH}/*.tmp
if_exist=$?

if [ $if_exist -ne 0 ]; then
    run_spark ${FILE_PATH}
    test_status $?
else
    echo "flume still transport data"
    sleep 150s
	for i in $(seq 1 90);
    do
        test_file ${FILE_PATH}
        if [ $? -eq 0 ]; then
            run_spark ${FILE_PATH}
            test_status $?
        fi
    done
    echo "tmp data file still exsit, please check flume or hdfs file"
    exit 7
fi
