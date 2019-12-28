#!/bin/bash
hostname="OwnHost"
port="Own_port"
username="username"
password="Password"
database="sample_database"
curr_dir=`pwd`
event_day=$1
sec=`date -d $1 +%s`
sec_daysbefore=$((sec - 86400))
event_yesterday=`date -d @$sec_daysbefore +%Y%m%d`

#根据输入进来的变量值，从配置表中读取设好的配置
select_sql="SELECT source_ulr,sql_name,db_user,db_user_pw,target_hdfs_path,target_ods_path FROM cfg_ods_dataset WHERE dataset_id=$2 AND group_id=$3 AND target_file_name='$4';"
# result=`mysql -h${hostname} -P${port} -u${username} -p${password} ${database} -e "${select_sql}" -s`

# echo $select_sql
if [[ ! -n "$1" || ! -n "$2" || ! -n "$3" || ! -n "$4" ]]; then
  echo "There are no 4 variables inputed, please input at lease 4 variables!"
  exit 1
else
  echo "The parameters you input are: $@"

  if echo $event_day | grep -Eq "[0-9]{4}[0-9]{2}[0-9]{2}" && date -d $event_day +%Y%m%d > /dev/null 2>&1
   then :;
  else
   echo "输入的是非日期数据值或者日期格式不正确，应为yyyymmdd，比如20111001";
   exit 1;
  fi;

  # get spark variable and execute coresponding shell scripts
  while read line
  do
    dataset_id=$2
    group_id=$3
    target_file_name=$4
    source_ulr=`echo $line | awk '{print $1}'`
    sql_name=`echo $line | awk '{print $2}'`
    source_sql_path=`echo "${curr_dir}/../src/${sql_name}"`
    db_user=`echo $line | awk '{print $3}'`
    db_user_pw=`echo $line | awk '{print $4}'`
    target_hdfs_path=`echo $line | awk '{print $5}'`
	target_ods_path=`echo $line | awk '{print $6}'`
    
    echo "dateset_id:${dataset_id},group_id:${group_id},target_file_name:${target_file_name},source_ulr:${source_ulr},source_sql_path:${source_sql_path},target_hdfs_path:${target_hdfs_path},db_user:${db_user}"
    # echo "bash ods_rs_media_tbb_building_info.sh ${event_day} ${source_ulr} ${source_sql_path} ${db_user} ${db_user_pw} ${target_hdfs_path}"
    bash ${curr_dir}/../src/load_data_to_ods.sh ${event_day} ${source_ulr} ${source_sql_path} ${db_user} ${db_user_pw} ${target_hdfs_path} ${target_ods_path} ${target_file_name}
    status=$?
    if [ $status -eq 0 ]; then
      echo "Successful: load data of ${event_yesterday} to ods hive table ${target_file_name} sucessful!!!!"
    else
      echo "Error: load data of ${event_yesterday} to ods hive table ${target_file_name} failed!!!!"
      python3 ${curr_dir}/../../Python/ding_talk_warning_report_py/main/ding_talk_with_agency.py 53
      exit 1
    fi
   
  done< <(mysql -h${hostname} -P${port} -u${username} -p${password} ${database} -e "${select_sql}" -s)

fi

