#!/bin/bash
#定义前一天日期，第几周，小时的变量并赋值
mydate=${1}
mydate=$(date -d"1 day ago ${mydate}" +%Y%m%d)
event_week=$(date -d ${mydate}  +%V)
event_hour=${2}
cur_dir=`pwd`

sql=${cur_dir}/../src/ADB_自定义.sql #需要改动，SQL脚本和sql脚本的路径根据自己需求定义

echo ${sql}
echo ${mydate}
echo ${event_week}
echo ${event_hour}



#source_path 需要改动
#把要执行的sql语句的关联表对应的路径列在下面，在跑SQL语句前，先验证一下这些路径下有没有文件
source_path1=/hive/warehouse/ods/sample1/event_week=${event_week}/event_day=${mydate}/event_hour=${event_hour}/_SUCCESS
source_path2=/hive/warehouse/ods/sample2/event_week=${event_week}/event_day=${mydate}/event_hour=${event_hour}/_SUCCESS
source_path3=/hive/warehouse/ods/sample3/event_week=${event_week}/event_day=${mydate}/event_hour=${event_hour}/_SUCCESS

#输出Source_path路径下文件是否都存在
for((i=1;i<=3;i++)); #需要修改 3可以根据有多少个source_path而改变
do
    mysource_path=`eval echo '$'"source_path$i"`
    # echo ${mysource_path}
    hadoop fs -test -e "$mysource_path"
    hdfs_flag=$?
    if [ $hdfs_flag -eq 0 ];then
        echo "mysource_path$i: $mysource_path hdfs file_data is exist!"
    else
        echo "mysource_path$i: $mysource_path hdfs file_data is not exist"
    fi
done
#判断Source_path路径下文件是否都存在
function testfile()
{
    local total_success_flag=0
    
    for((i=1;i<=3;i++)); #需要修改 3可以根据有多少个source_path而改变
    do
            mysource_path=`eval echo '$'"source_path$i"`
            hadoop fs -test -e $mysource_path
            hdfs_flag=$?
            ((total_success_flag+=hdfs_flag))
    done
    echo $total_success_flag
}

total_success_result=$(testfile)
echo "total not exists files: $total_success_result"
n=0
while (($total_success_result != 0))
do
    sleep 1m
    ((n++))
    if [ $n -gt 60 ]; then 
        echo "等待1个小时，系统数据还没加载到hdfs"
        cd ${cur_dir}/../../Python/ding_talk_warning_report_py/main/
        python3 ding_talk_with_agency.py 32  # 上面是之前Python目录分享的发钉钉脚本
        exit 7
    fi
    
    total_success_result=$(testfile)
    echo "total not exists files: $total_success_result"
done

#target_path需要改动
target_success_file=/hive/warehouse/dwd/target_path/event_week=${event_week}/event_day=${mydate}/event_hour=${event_hour}/_SUCCESS

#使用spark-sql跑hive SQL脚本，指定所需资源，shuffle分区数
spark-sql  --master yarn --deploy-mode client --executor-memory 4G --executor-cores 1 --num-executors 4 --queue etl -S --conf spark.sql.shuffle.partitions=8 --conf spark.default.parallelism=8 -hiveconf event_week=${event_week} -hiveconf event_day=${mydate} -hiveconf event_hour=${event_hour} -f ${sql}

status=$?
if [ $status ==  0 ]; then
    hadoop fs -touchz ${target_success_file}
    echo "OK"
   
else 
    echo "failed"
    cd ${cur_dir}/../../Python/ding_talk_warning_report_py/main/
    python3 ding_talk_with_agency.py 33
    exit 9
fi
