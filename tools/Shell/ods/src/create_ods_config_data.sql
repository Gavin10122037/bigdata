-- prod
-- kuma compose plan
-- insert into ods_kuma_compose_plan
create table cfg_ods_dataset(
	dataset_id  bigint comment '数据集组id',
	dataset_name  varchar(100) comment '数据集组名称',
	source_table  varchar(100) comment '源数据表名',
	sql_name varchar(100) comment '拉去源数据库中表的SQL文件名',
	source_ulr varchar(200) comment '数据库连接信息',
	target_file_name varchar(100) comment '拉取到HDFS上的目标文件名',
	target_hdfs_path varchar(100) comment '目标文件在hdfs上的路径',
	group_id  int comment '数据集下数据组',
	sequenceid int comment '排序id',
	is_enable  int comment '是否可用，1：可用，0：不可用',
	create_time datetime comment '创建时间',
	update_time datetime comment '更新时间',
	db_user varchar(100) comment '数据库用户',
	db_user_pw varchar(100) comment '数据库密码',
	target_hdfs_path varchar comment '目标文件对应的hive中ods层表的存放路径'
)
insert into cfg_ods_dataset(dataset_id,dataset_name,source_table,sql_name,source_ulr,target_file_name,target_hdfs_path,group_id,sequenceid,is_enable,create_time,update_time,db_user,db_user_pw,target_ods_path)
select 2 as dataset_id,'sample','sample_table' as source_table,'ods_sample.sql' as sql_name,'jdbc:mysql://host:port/database' source_ulr,
  'ods_sample_file' as target_file_name,
	'/source/ods/sample/ods_sample_file' as target_hdfs_path, 1 as group_id, 1 as sequenceid,1 as is_enable,now() as create_time, now() as update_time,
	'usename' as db_user,'password' as db_user_pw,'/hive/warehouse/ods/sample/ods_sample_file' as target_ods_path
