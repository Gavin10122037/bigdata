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

#  get conn,topic为数据库主题：有液晶fmc数据库,有数码fmc数据库，有素材库creative
def getcon(topic):
    if topic == 'creative':
        host=''
        port=
        user='dw_user'
        passwd=''
        db='creative'
        charset='utf8'
    elif topic == 'lcd':
        host=''
        port=
        user='dw_user_reader'
        passwd=''
        db='fmc_insertion'
        charset='utf8'
    elif topic == 'smart':
        host=''
        port=
        user='dw_user_reader'
        passwd=''
        db='fmc_insertion'
        charset='utf8'
    elif topic == 'framedia':
        host=''
        port=
        user='dw_user_reader'
        passwd=''
        db='fmc_insertion'
        charset='utf8'
    try:
        conn = pymysql.Connect(host=host,port=port,database=db,user=user,password=passwd,charset=charset)
        conn_str='mysql+pymysql://%s'%(user)+':%s'%(passwd) +'@%s'%(host)+':%s'%(port) +'/%s'%(db) + '?charset=%s'%(charset)
        db_engine=create_engine(conn_str,echo=False,encoding="utf-8")
        print(conn_str)
        return conn,db_engine
    except Exception as e:
        print(e)
        send_email("数据连接失败","加载素材数据失败")
        n = [0,19]
        ding.main(n)

# 发送邮件
def send_email(mail_msg,mail_subject):
    mail_host = ''
    mail_port = 25
    mail_user = ''
    mail_passwd = ''

    sender = '#'
    receivers = ['##','##']

    try:
        # 三个参数：第一个为文本内容，第二个 plain 设置文本格式，第三个 utf-8 设置编码
        message = MIMEText(mail_msg,'html','utf-8')
        message['From'] = sender # 括号里对应发件人邮箱昵称、发件人邮箱账号
        message['To'] = "#" # 括号里对应收件人邮箱昵称、收件人邮箱账号
        message['Cc'] = "#,#"
        message['Subject'] = mail_subject

        server = smtplib.SMTP(mail_host,mail_port)  #发件人邮箱中的SMTP服务器，端口是25
        server.login(mail_user,mail_passwd) #发件人邮箱用户和密码
        server.sendmail(sender,message['To'].split(",") + message['Cc'].split(","),message.as_string()) # 第三个参数，msg 是字符串，表示邮件。我们知道邮件一般由标题，发信人，收件人，邮件内容，附件等构成，发送邮件的时候，要注意 msg 的格式。这个格式就是 smtp 协议中定义的格式
    except smtplib.SMTPException:
        print("Error: 无法发送邮件")
        n = [0,20]
        ding.main(n)

#ftp配置信息,返回ftp对象
def get_ftp(ftp_type):
    if ftp_type == 'lcd':
        host = '#'
        port = 21 
        username = ''
        password = '553f58d3'
    elif ftp_type == 'smart':
        host = '#'
        port = 21 
        username = ''
        password = ''
    elif ftp_type == 'creative':
        host = '#'
        port = 21
        username = ''
        password = ''

    #创建ftp实例对象
    ftp=ftplib.FTP()
    #打开调试级别2，显示详细信息；0为关闭调试信息
    ftp.set_debuglevel(2)
    try:
        #连接
        ftp.connect(host,port)
        #登录
        ftp.login(username,password)
        return ftp
    except Exception as e:
        print(e)
        send_email("登录出错","加载素材数据失败")
        n = [0,21]
        ding.main(n)


#FTP下载文件，ftp:ftp连接对象，file_local:下载到本地文件名+路径，remote_filename:下载远程ftp上文件名
def ftp_download(ftp,file_local,remote_filename):
    bufsize = 1024 #设置缓冲块大小
    try:
        file_handler = open(file_local,'wb').write #以写模式在本地打开文件
        ftp.retrbinary('RETR %s'%(remote_filename.encode('utf-8').decode('latin1')),file_handler,bufsize) #如果素材名是中文，需要encode成utf8,因为python ftp客户端只默认支持Latin1格式
    except Exception as e:
        print(e)
        send_email("%s下载失败"%(remote_filename),"加载素材数据失败")

#FTP上传文件,ftp:ftp连接对象，file_local:本地文件名+upload_filename:上传远程ftp上文件名
def ftp_upload(ftp,file_local,upload_filename):
    bufsize = 1024 #设置缓冲块大小
    try:
        file_handler = open(file_local,'rb')
        ftp.storbinary('STOR %s'%(upload_filename.encode('utf-8').decode('latin1')),file_handler,bufsize)
    except Exception as e:
        print(e)
        send_email("%s上传素材服务器失败"%(upload_filename),"加载素材数据失败")
