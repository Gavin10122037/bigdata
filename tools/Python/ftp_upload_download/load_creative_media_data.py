# -*- coding:utf-8 -*-
"""
    author:jianggongqing
    function:load media data
    create date:2019-10-31
    modify date:2019-12-16
    add function: add send dingding message
"""
import pymysql
import pandas as pd
import sys
import json
import os
import xlrd
import datetime
import calendar 
import time
import smtplib
from email.mime.text import MIMEText
from email.header import Header 
from sqlalchemy import create_engine
import ftplib
from ding_talk_warning_report_py.main import ding_talk_with_agency as ding
import config 

#   获取昨天日期
def get_previous_date(date):
    one_day = datetime.timedelta(days=1)
    predate=(date - one_day).strftime('%Y-%m-%d')
    return predate


#插入素材数据进MySQL中素材库
def insertData(predate,local_path,product_line,tablename):
    #调用链接到素材creative的函数，返回conn和cur1
    creative_conn,creative_engine=config.getcon('creative')
     #调用链接到液晶fmc的函数，返回conn和cur1
    fmc_conn,fmc_engine=config.getcon(product_line)
    #调用链接到数码fmc的函数，返回conn和cur1
    creative_cur = creative_conn.cursor()

    sqlcmd = """select type,prgid as prg_id,vid,vfn,signature,filepath as file_path,size,starttime as start_time,
        endtime as end_time,inserttime as insert_time_source,state from media_data where date(inserttime) = '%s'
     """
    sql1=sqlcmd%(predate)
    print(sql1)
    fmc_data=pd.read_sql(sql1,fmc_conn)
    print(fmc_data)
    
    #获取日期格式20191011这种类型
    predate_list = predate.split('-') 
    format_predate = ''.join(predate_list)
    
    #ftp上下载液晶素材
    source_ftp = config.get_ftp(product_line) #获取液晶LCD ftp
    #ftp上传液晶素材到素材服务器上
    creative_ftp = config.get_ftp('creative') #获取素材服务器creative ftp
    download_count = 0 #下载素材个数
    upload_count = 0 #上传素材个数

    for index,row in fmc_data.iterrows():
        ftp_file = row['file_path']
        source_path = os.path.split(ftp_file)
        str_split = ftp_file.split("/")
        source_filename = str_split[-1]
        source_filepath = source_path[0]
        try:
            source_ftp.cwd(source_filepath) #切换到source ftp目录
        except:
            print("已切换")
        finally:
            #根据日期生成目录，并判断如果目录不存在，则生成改目录
            local_dir = local_path + '/%s/%s'%(format_predate,str_split[2])
            if not os.path.exists(local_dir):
                os.makedirs(local_dir)
            file_local = local_dir + '/%s'%(source_filename) #本地中存放路径+文件名
            config.ftp_download(source_ftp,file_local,source_filename) #从FTP下载文件到本地
            download_count = download_count + 1 #素材下载成功+1
            #素材服务器上远程路径
            product_path = '/data/creative/%s'%(product_line) #product_line级别
            date_path = product_path + '/%s'%(format_predate) #日期级别目录
            creative_path = date_path + '/%s'%(str_split[2])
            
            try:
                creative_ftp.cwd(creative_path) #切换到素材服务器远程目录
            except ftplib.error_perm:
                try:
                    creative_ftp.mkd(product_path) #创建远程目录
                    creative_ftp.cwd(creative_path) #切换到素材服务器远程目录
                except ftplib.error_perm:
                    try:
                        creative_ftp.mkd(date_path) #创建远程目录
                        creative_ftp.cwd(creative_path) #切换到素材服务器远程目录
                    except ftplib.error_perm:
                        try:
                            creative_ftp.mkd(creative_path) #创建远程目录
                            creative_ftp.cwd(creative_path) #切换到素材服务器远程目录
                        except ftplib.error_perm:
                            print('you have no authority to make dir')
            except AttributeError:
                print("已切换")
            finally:
                #上传素材到素材服务器
                config.ftp_upload(creative_ftp,file_local,source_filename) 
                upload_count = upload_count + 1 #素材上传成功+1
                creative_file = creative_path + '/%s'%(source_filename)
                row['file_path']=creative_file #修改dataframe中file_path列值为素材服务器上的路径
                #插入mysql素材库
                delete_sql = "delete from %s where vfn = '%s';"%(tablename,row['vfn'])
                creative_engine.execute(delete_sql) #执行SQL语句
                insert_sql = "insert into %s(type,prg_id,vid,vfn,signature,file_path,size,start_time,end_time,insert_time_source,state) values('%s','%s','%s','%s','%s','%s','%s','%s','%s','%s',%s);"
                insert_sql = insert_sql%(tablename,row['type'],row['prg_id'],row['vid'],row['vfn'],row['signature'],row['file_path'],row['size'],row['start_time'],row['end_time'],row['insert_time_source'],row['state'])
                creative_engine.execute(insert_sql) #执行SQL语句
    
    
    source_ftp.set_debuglevel(0)
    creative_ftp.set_debuglevel(0)
    creative_cur.close()
    creative_conn.close()
    fmc_conn.close()
    if upload_count == download_count:
        config.send_email("加载%s素材%s数据成功,总共上传素材数量是%s"%(product_line,str(format_predate),str(upload_count)),"加载%s素材%s数据成功"%(product_line,str(format_predate)))
        print("%s ftp dwon and upload %s to creative OK"%(product_line,str(format_predate)))
    else:
        msg = "加载%s素材%s数据失败,总共下载素材数量是%s,总共上传素材数量是%s"%(product_line,str(format_predate),str(download_count),str(upload_count))
        config.send_email(msg,"加载%s素材%s数据失败"%(product_line,str(format_predate)))
        print("%s ftp dwon and upload %s to creative fail"%(product_line,str(format_predate)))
        n = [0,22]
        ding.main(n)

def main(argv):
    date = datetime.datetime.strptime(argv[1],'%Y%m%d')
    #date = datetime.datetime.strptime('2019-10-10','%Y-%m-%d')
    print(date)
    predate=get_previous_date(date)
    #上传液晶lcd-fam素材
    product_line = 'lcd'
    local_path = '/data/creative/%s'%(product_line)
    tablename = 'ods_media_data_lcd_fam' 
    insertData(predate,local_path,product_line,tablename) 
    #上传液晶smart-fdm素材
    product_line = 'smart'
    local_path = '/data/creative/%s'%(product_line)
    tablename = 'ods_media_data_smart_fdm' 
    insertData(predate,local_path,product_line,tablename) 

if __name__ == "__main__":
    main(sys.argv)



