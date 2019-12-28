#!/usr/bin/env bash
#sh  test.sh host db  user passwd target_hdfs


mydate=${1}
mydate=$(date -d"1 day ago ${mydate}" +%Y%m%d)
eventweek=$(date -d ${mydate}  +%V)
host=$2
db_file=$3
user=$4
passwd=$5
target_hdfs=$6/$mydate
target_ods=$7
target_ods_table=$8


db=`cat ${db_file} | tr "\n" " " | tr "\r" " "`
db=`printf "${db}" "${mydate}" "${mydate}" "${mydate}"`
echo "mydate: $mydate"
echo "eventweek: $eventweek"
echo "host: $host"
echo "db: $db"
echo "table: $table"
echo "target_hdfs: $target_hdfs"
echo "target_ods: $target_ods"
echo "target_ods_table: $target_ods_table"

hadoop fs -test -e $target_hdfs
hdfs_flag=$?

echo "status:$hdfs_flag"
if [ $hdfs_flag -eq 0 ];then
  echo "hdfs file_data  is exist!"
  hadoop fs -rm -r $target_hdfs  && echo "spark.read.format(\"jdbc\").option(\"url\",\"${host}\" ).option(\"user\", \"${user}\").option(\"password\",\"${passwd}\").option(\"dbtable\", \"${db}\").load().write.parquet(\"${target_hdfs}\")" > operation  && cat operation | spark-shell --master yarn  --deploy-mode client --executor-memory 2G --executor-cores 1 --num-executors 4 --queue etl

else
   echo "hdfs file_data is not exist" && echo "spark.read.format(\"jdbc\").option(\"url\",\"${host}\" ).option(\"user\", \"${user}\").option(\"password\",\"${passwd}\").option(\"dbtable\", \"${db}\").load().write.parquet(\"${target_hdfs}\")" > operation  && cat operation | spark-shell --master yarn  --deploy-mode client --executor-memory 2G --executor-cores 1 --num-executors 4 --queue etl

fi

## echo "spark.read.format(\"jdbc\").option(\"url\",\"${host}\" ).option(\"user\", \"${user}\").option(\"password\",\"${passwd}\").option(\"dbtable\", \"${db}\").load().write.parquet(\"${target_hdfs}\")" > operation  && cat operation | spark-shell --master yarn  --deploy-mode client --executor-memory 2G --executor-cores 1 --num-executors 4

hdfs_path=$target_hdfs/_SUCCESS
echo "hdfs_path: $hdfs_path"
hadoop fs -test -e $hdfs_path
hdfs_success_flag=$?

if [[ $hdfs_success_flag -eq 0 ]];then
   echo "spark job unload to hdfs run successfully"
   
   echo "spark job load to ods start"
   echo "load data inpath '${target_hdfs}/*' overwrite into table dw.${target_ods_table} partition(event_week=${eventweek},event_day='${mydate}',event_hour='00');" | hive
   
   ods_path=$target_ods/event_week=${eventweek}/event_day=${mydate}/event_hour=00/_SUCCESS
   echo "ods_path: $ods_path"
   hadoop fs -test -e $ods_path
   ods_success_flag=$?
   if [[ $ods_success_flag -eq 0 ]];then
        echo "spark job load to ods run successfully"
   else
       echo "spark job load to ods run failed"
       exit 7
   fi

else
   echo "spark job unload to hdfs run failed"
   exit 7
fi

