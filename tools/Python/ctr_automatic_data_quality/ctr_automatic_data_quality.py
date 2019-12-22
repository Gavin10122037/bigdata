'''
    function: check data检查数据a同步给s的数据正确性
    date: 2019-09-25
'''
import pymssql
import pymysql
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

#   获取下周一日期
def get_monday_date():
    today = datetime.date.today()
    one_day = datetime.timedelta(days=1)
    monday_num = calendar.MONDAY
    if today.weekday() < 5:
        while today.weekday() != monday_num:
            today -= one_day
        cur_monday = today.strftime('%Y%m%d')
        return cur_monday
    else: 
        while today.weekday() != monday_num:
            today += one_day
        next_monday = today.strftime('%Y%m%d')
        return next_monday

#   执行SQL
def get_sql_results(sql,topic):
    if topic == 'dw':
        host='#'
        port=13300
        user='dw_user'
        passwd=''
        db='dw'
        charset='utf8'
    elif topic == 'kuma':
        host='#'
        port=10001
        user='LTAIdZrQMI0bWfpj'
        passwd=''
        db='kuma_data_center_online'
        charset='utf8'
    elif topic == 'ctr':
        host='#'
        port=13300
        user='dw_user_reader'
        passwd=''
        db='fdm_monitor_report'
        charset='utf8'
    
    conn = pymysql.Connect(host=host,port=port,database=db,user=user,password=passwd,charset=charset)
    cur1 = conn.cursor()
    cur1.execute(sql)
    data = cur1.fetchall()
    print(data)

    cur1.close()
    conn.close()
    return data 

# 发送邮件
def send_email(mail_msg,mail_subject):
    mail_host = '#'
    mail_port = 25
    mail_user = 'dw_report'
    mail_passwd = ''

    sender = '#'
    receivers = ['#','#']

    try:
        # 三个参数：第一个为文本内容，第二个 plain 设置文本格式，第三个 utf-8 设置编码
        message = MIMEText(mail_msg,'html','utf-8')
        message['From'] = sender # 括号里对应发件人邮箱昵称、发件人邮箱账号
        message['To'] = "#,#" # 括号里对应收件人邮箱昵称、收件人邮箱账号
        message['Cc'] = "#,#"
        message['Subject'] = mail_subject

        server = smtplib.SMTP(mail_host,mail_port)  #发件人邮箱中的SMTP服务器，端口是25
        server.login(mail_user,mail_passwd) #发件人邮箱用户和密码
        server.sendmail(sender,message['To'].split(",") + message['Cc'].split(","),message.as_string()) # 第三个参数，msg 是字符串，表示邮件。我们知道邮件一般由标题，发信人，收件人，邮件内容，附件等构成，发送邮件的时候，要注意 msg 的格式。这个格式就是 smtp 协议中定义的格式
    except smtplib.SMTPException:
        print("Error: 无法发送邮件")

#   主函数，数据校验
def main():
    monday = get_monday_date()
    results_dict = dict()  #记录校验结果

    #  1. 比较DW与KUMA的点位数与城市数
    #  dw总行数，点位数
    dw_sql = """
            select count(distinct location_id) as dw_location_count,count(distinct city_id) as dw_city_count
            from compose_plan_"""+monday+""" a
            order by dw_location_count,dw_city_count;
        """
    dw_data = get_sql_results(sql=dw_sql,topic='dw')
    for row in dw_data:
        dw_location_count = row[0]
        dw_city_count = row[1]

    #  kuma 点位数
    kuma_sql = """
            select count(distinct location_id) as kuma_location_count,count(distinct city_id) as kuma_city_count
            from compose_plan 
            where if(weekday(curdate()) <= 4,date_format(date_add(date_sub(curdate(),INTERVAL WEEKDAY(curdate()) DAY),interval +0 DAY),'%Y-%m-%d')
                ,date_format(date_add(date_sub(curdate(),INTERVAL WEEKDAY(curdate()) DAY),interval +7 DAY),'%Y-%m-%d'))  between ad_cycle_start_date and ad_cycle_end_date -- + 7 DAY
            AND schedule_end_date >= if(weekday(curdate()) <= 4,date_add(date_sub(curdate(),INTERVAL WEEKDAY(curdate()) DAY),interval +0 DAY)
                ,date_add(date_sub(curdate(),INTERVAL WEEKDAY(curdate()) DAY),interval +7 DAY))
            AND schedule_start_date <= if(weekday(curdate()) <= 4,date_add(date_sub(curdate(),INTERVAL WEEKDAY(curdate()) DAY),interval +6 DAY)
                ,date_add(date_sub(curdate(),INTERVAL WEEKDAY(curdate()) DAY),interval +13 DAY));
        """
    kuma_data = get_sql_results(sql=kuma_sql,topic='kuma')
    for row in kuma_data:
        kuma_location_count = row[0]
        kuma_city_count = row[1]

    # 判断dw中数据与Kuma中点位和城市数据行数是否相同
    if  dw_location_count == kuma_location_count and dw_city_count == kuma_city_count:
        mail_subject = "kuma中点位数与城市数相同，dw点位数和城市数正常"
        mail_msg = """
            <p>dw总数据与Kuma中点位数和城市数相同，Kuma同步数据到dw正常</p> 
        """
        results_dict["1.比较DW与KUMA的点位数与城市数"] = "kuma中点位数与城市数相同，dw点位数和城市数正常"
        # send_email(mail_msg,mail_subject)
    else:
        mail_subject = "kuma中点位数与城市数不相同，dw点位数和城市数异常"
        mail_msg = """
            <p>dw总数据与Kuma中点位数和城市数不相同,<br>
            Kuma中点位数为"""+str(kuma_location_count)+""",dw中点位数为"""+str(dw_location_count)+""",<br>
            kuma中城市数为"""+str(kuma_city_count) +""",dw中城市数为"""+str(dw_city_count)+""",<br>
            请查检查原因</p> 

        """
        results_dict["1.比较DW与KUMA的点位数与城市数"] = "kuma中点位数与城市数不相同，dw点位数和城市数异常"
        send_email(mail_msg,mail_subject)

    # 2. 比较dw和CTR的总行数与点位数
    #  dw总行数，点位数
    dw_sql = """
            select count(*) as dw_total_rows,count(distinct location_id) as dw_location_count,count(distinct city_id) as dw_city_count
            from compose_plan_"""+monday+""" 
            order by dw_total_rows,dw_location_count,dw_city_count;
        """
    dw_data = get_sql_results(sql=dw_sql,topic='dw')
    for row in dw_data:
        dw_total_rows = row[0]
        dw_location_count = row[1]
        dw_city_count = row[2]

    #  ctr总行数，点位数
    ctr_sql = """
            select count(*) as ctr_total_rows,count(distinct location_id) as ctr_location_count,count(distinct city_id) as ctr_city_count
            from compose_plan_"""+monday+"""
            order by ctr_total_rows,ctr_location_count,ctr_city_count;
        """
    ctr_data = get_sql_results(sql=ctr_sql,topic='ctr')
    for row in ctr_data:
        ctr_total_rows = row[0]
        ctr_location_count = row[1]
        ctr_city_count = row[2]
    
    # 判断dw中数据与ctr中数据行数是否相同
    if dw_total_rows == ctr_total_rows and dw_location_count == ctr_location_count and dw_city_count == ctr_city_count:
        mail_msg = """
            <p>dw总数据与ctr中数据行数相同</p> 
        """
        results_dict["2.比较dw和CTR的总行数与点位数"] = "dw总行数与ctr中总行数相同,CTR总行数正常"
        # send_email(mail_msg)
    else:
        mail_subject = "dw总行数与ctr中数据行数不相同，CTR总行数异常"
        mail_msg = """
            <p>dw总行数与ctr中数据行数不相同，<br>
            dw总行数为"""+str(dw_total_rows)+""",ctr总行数为"""+str(ctr_total_rows)+""",<br>
            dw点位数为"""+str(dw_location_count)+""",ctr点位数为"""+str(ctr_location_count)+""",<br>
            dw城市数为"""+str(dw_city_count)+""",ctr城市数为"""+str(ctr_city_count)+""",<br>
            请查检查原因</p> 
        """
        results_dict["2.比较dw和CTR的总行数与点位数"] = "dw总行数与ctr中数据行数不相同，CTR总行数异常"
        send_email(mail_msg,mail_subject)

    # 3. KA客户监测品牌不能为空
    # dw中监测
    dw_sql = """
        select count(*)
        from  compose_plan_"""+monday+"""
        where ifnull(ad_product,'') = '' and account_type = 'INNER_CUSTOMER';
    """
    dw_data = get_sql_results(sql=dw_sql,topic='dw')
    for row in dw_data:
        dw_count = row[0] # dw中KA客户监测品牌为空的数量

    # Kuma中监测
    kuma_sql = """
        select count(*) 
        from compose_plan 
        where account_type = 'INNER_CUSTOMER' and ifnull(brand,'') = ''
        and if(weekday(curdate()) <= 4,date_format(date_add(date_sub(curdate(),INTERVAL WEEKDAY(curdate()) DAY),interval +0 DAY),'%Y-%m-%d')
            ,date_format(date_add(date_sub(curdate(),INTERVAL WEEKDAY(curdate()) DAY),interval +7 DAY),'%Y-%m-%d'))  between ad_cycle_start_date and ad_cycle_end_date -- + 7 DAY
        AND schedule_end_date >= if(weekday(curdate()) <= 4,date_add(date_sub(curdate(),INTERVAL WEEKDAY(curdate()) DAY),interval +0 DAY)
            ,date_add(date_sub(curdate(),INTERVAL WEEKDAY(curdate()) DAY),interval +7 DAY))
        AND schedule_start_date <= if(weekday(curdate()) <= 4,date_add(date_sub(curdate(),INTERVAL WEEKDAY(curdate()) DAY),interval +6 DAY)
            ,date_add(date_sub(curdate(),INTERVAL WEEKDAY(curdate()) DAY),interval +13 DAY));
    """

    kuma_data = get_sql_results(sql=kuma_sql,topic='kuma')
    for row in kuma_data:
        kuma_count = row[0] # dw中KA客户监测品牌为空的数量

    # 判断dw中KA客户监测品牌为空的数量
    if  dw_count == 0 and kuma_count == 0:
        mail_msg = """
            <p>KA客户监测品牌都不为空，监测品牌数据正常</p> 
        """
        results_dict["3.KA客户监测品牌是否为空"] = "KA客户监测品牌都不为空，ctr监测品牌数据正常"
        # send_email(mail_msg)
    else:
        mail_subject = "KA客户监测品牌有为空，监测品牌数据异常"
        mail_msg = """
            <p>KA客户监测品牌有"""+str(dw_count) +"""条数据为空，Kuma监测品牌数据异常</p> 
        """
        results_dict["3.KA客户监测品牌是否为空"] = "KA客户监测品牌有为空，ctr监测品牌数据异常"
        send_email(mail_msg,mail_subject)

    # 4. ctr中素材品牌为空的数量
    # ctr中监测
    ctr_sql = """
        select count(*)
        from  compose_plan_"""+monday+"""
        where ifnull(creative_brand,'') = '' and ifnull(ad_product,'') <> '';
    """
    ctr_data = get_sql_results(sql=ctr_sql,topic='ctr')
    for row in ctr_data:
        ctr_count = row[0] # ctr中素材品牌为空的数量

    # 判断ctr中素材品牌为空的数量
    if  ctr_count == 0:
        mail_msg = """
            <p>ctr中监测品牌不为空时，素材品牌也不为空，ctr素材品牌数据正常</p> 
        """
        results_dict["4.监测ctr中素材品牌是否有为空"] = "ctr素材品牌不为空，ctr素材品牌数据正常"
        # send_email(mail_msg)
    else:
        mail_subject = "ctr素材品牌有为空，ctr素材品牌数据异常"
        mail_msg = """
            <p>ctr中监测品牌不为空时，有"""+str(ctr_count)+"""条数据素材品牌为空，ctr素材品牌数据异常,需要监测标准品牌数据是否同步ok</p> 
        """
        results_dict["4.监测ctr中素材品牌是否有为空"] = "ctr素材品牌有为空，ctr素材品牌数据异常"
        send_email(mail_msg,mail_subject)

    # 5. ctr中素材时长是否为整数，如果为整数，是正确，不是，错误
    # ctr中监测
    ctr_sql = """
        select count(*)
        from compose_plan_"""+monday+"""
        where cast(creative_duration as signed) <> creative_duration;
    """
    ctr_data = get_sql_results(sql=ctr_sql,topic='ctr')
    for row in ctr_data:
        ctr_count = row[0] # ctr中素材时长不为整数的数量

    # 判断ctr中素材时长不为整数的数量
    if  ctr_count == 0:
        mail_msg = """
            <p>ctr中素材时长为整数，ctr素材时长数据正常</p> 
        """
        results_dict["5.监测ctr中素材时长是否为整数"] = "ctr中素材时长为整数，ctr素材时长数据正常"

        #send_email(mail_msg)
    else:
        mail_subject = "ctr中素材时长不为整数，ctr素材时长数据异常"
        mail_msg = """
            <p>ctr中素材时长不为整数，ctr素材时长数据异常，请相关数仓人员确认解决</p> 
        """
        results_dict["5.监测ctr中素材时长是否为整数"] = "ctr中素材时长不为整数，ctr素材时长数据异常"
        send_email(mail_msg,mail_subject)

     # 6. 监测ctr中合同频次与Kuma中合同频次是否相同
    # ctr中监测
    ctr_sql = """
        select distinct frequency
        from compose_plan_"""+monday+"""
        order by frequency;
    """
    ctr_data = get_sql_results(sql=ctr_sql,topic='ctr')

     # Kuma中监测
    kuma_sql = """
        select distinct frequency
        from compose_plan 
        where if(weekday(curdate()) <= 4,date_format(date_add(date_sub(curdate(),INTERVAL WEEKDAY(curdate()) DAY),interval +0 DAY),'%Y-%m-%d')
            ,date_format(date_add(date_sub(curdate(),INTERVAL WEEKDAY(curdate()) DAY),interval +7 DAY),'%Y-%m-%d'))  between ad_cycle_start_date and ad_cycle_end_date -- + 7 DAY
        AND schedule_end_date >= if(weekday(curdate()) <= 4,date_add(date_sub(curdate(),INTERVAL WEEKDAY(curdate()) DAY),interval +0 DAY)
            ,date_add(date_sub(curdate(),INTERVAL WEEKDAY(curdate()) DAY),interval +7 DAY))
        AND schedule_start_date <= if(weekday(curdate()) <= 4,date_add(date_sub(curdate(),INTERVAL WEEKDAY(curdate()) DAY),interval +6 DAY)
            ,date_add(date_sub(curdate(),INTERVAL WEEKDAY(curdate()) DAY),interval +13 DAY))
        order by frequency;
    """
    kuma_data = get_sql_results(sql=kuma_sql,topic='kuma')

    # 判断ctr中素材时长不为整数的数量
    if  ctr_data == kuma_data:
        mail_msg = """
            <p>ctr与Kuma中的合同频次相同,ctr中合同频次数据正常</p> 
        """
        results_dict["6.监测ctr中合同频次与Kuma中合同频次是否相同"] = "ctr与Kuma中的合同频次相同,ctr中合同频次数据正常"
        # send_email(mail_msg)
    else:
        mail_subject = "ctr与Kuma中的合同频次不相同,ctr中合同频次数据异常"
        mail_msg = """
            <p>ctr与Kuma中的合同频次不相同,
            kuma中的合同频次是"""+str(kuma_data)+""",<br>
            ctr中的合同频次是"""+str(ctr_data)+""",<br>
            ctr中合同频次数据异常，请相关数仓人员确认解决</p> 
        """
        results_dict["6.监测ctr中合同频次与Kuma中合同频次是否相同"] = "ctr与Kuma中的合同频次不相同,ctr中合同频次数据异常"
        send_email(mail_msg,mail_subject)

    # 7. 监测合同内的合同发布明细数据中的城市是否能与排片中数据相同
    # dw中监测
    dw_sql = """
        select distinct a.pb_region
        from tbb_publish_detail_data a
        where not exists(
            select * 
            from (
                select distinct a.city_id,b.AmapCityName,b.PbRegionCode,case when b.PbRegion = '郑州' and b.AmapCityName = '新郑市' then '新郑' else b.PbRegion end as PbRegion
                from compose_plan_"""+monday+""" a 
                inner join tbb_sale_fview3 b 
                on a.location_id = b.LocationId and b.pub_date = if(weekday(curdate()) <= 4,date_format(date_add(date_sub(curdate(),INTERVAL WEEKDAY(curdate()) DAY),interval +0 DAY),'%Y-%m-%d')
                    ,date_format(date_add(date_sub(curdate(),INTERVAL WEEKDAY(curdate()) DAY),interval +7 DAY),'%Y-%m-%d'))
                and a.from_type in ('合同内','特赠')
            ) d 
            where a.pb_region = d.PbRegion )
        and product_resource = '智能屏' and from_type in ('合同内','特赠')
        and created_time in (select max(created_time) from tbb_publish_detail_data) and a.pb_region <> '南通';
    """
    dw_data = get_sql_results(sql=dw_sql,topic='dw')
    city_list = []
    for row in dw_data:
        city_list.append(row[0]) # 合同发布中合同内比排片中多出的城市数量

    # 判断dw合同发布中合同内比排片中多出的城市数量
    if  city_list:
        mail_subject = "合同发布中合同内的销售城市比排片销售城市多,ctr中销售城市数据异常"
        mail_msg = """
            <p>合同发布中合同内的销售城市比排片销售城市多出"""+str(city_list)+"""，排片中销售城市数据异常，请检查合同发布明细和排片数据</p> 
        """
        results_dict["7.监测合同内的合同发布明细数据中的城市是否能与排片中数据相同"] = "合同发布中合同内的销售城市比排片销售城市多,ctr中销售城市数据异常"
        send_email(mail_msg,mail_subject)
    else:
        mail_msg = """
            <p>合同发布中合同内的销售城市与排片中销售城市相同，排片中销售城市数据正常</p> 
        """
        results_dict["7.监测合同内的合同发布明细数据中的城市是否能与排片中数据相同"] = "合同发布中合同内的销售城市与排片中销售城市相同，排片中销售城市数据正常"
        #send_email(mail_msg)


    # 8. 监测合同内的合同发布明细数据中的监测品牌是否能与排片中监测品牌相同
    # dw中监测
    dw_sql = """
        select distinct a.brand
        from tbb_publish_detail_data a
        where not exists(
            select * 
            from (
                select distinct a.city_id,a.ad_product
                from compose_plan_"""+monday+""" a 
            ) d 
            where a.brand = d.ad_product)
        and product_resource = '智能屏' and from_type in ('合同内','特赠')
        and created_time in (select max(created_time) from tbb_publish_detail_data);
    """
    dw_data = get_sql_results(sql=dw_sql,topic='dw')
    brand_list = []
    for row in dw_data:
        brand_list.append(row[0])  # 合同发布中合同内比排片中多出的监测品牌数量
    
    # 判断合同发布中合同内比排片中多出的监测品牌数量
    if  brand_list:
        mail_subject = "合同发布明细中监测品牌比排片监测品牌多,ctr中监测品牌数据异常"
        mail_msg = """
            <p>合同发布明细中监测品牌比排片监测品牌多出"""+str(brand_list)+"""，排片中监测品牌数据异常，请检查合同发布明细中多出品牌</p> 
        """
        results_dict["8.监测合同内的合同发布明细数据中的监测品牌是否与排片中监测品牌相同"] = "合同发布明细中监测品牌比排片监测品牌多,ctr中监测品牌数据异常"
        send_email(mail_msg,mail_subject)
    else:
        mail_msg = """
            <p>合同发布明细中监测品牌与排片中监测品牌相同，排片中监测品牌数据正常</p> 
        """
        results_dict["8.监测合同内的合同发布明细数据中的监测品牌是否与排片中监测品牌相同"] = "合同发布明细中监测品牌与排片中监测品牌相同，排片中监测品牌数据正常"
        # send_email(mail_msg)

    # 9. 监测一个素材是否对应多个素材品牌
    # dw中监测
    ctr_sql = """
        select creative_name
        from compose_plan_"""+monday+""" a 
        where creative_name not like '%江总%' and creative_name not like '%分众%'
        group by creative_name
        having count(distinct a.brand_code) > 1;
    """
    ctr_data = get_sql_results(sql=ctr_sql,topic='ctr')
    creative_list = []
    for row in ctr_data:
        creative_list.append(row[0])  # 合同发布中合同内比排片中多出的监测品牌数量
    
    print(str(creative_list))
    # 判断一个素材是否对应多个素材品牌
    if  creative_list:
        mail_subject = "有素材对应多个素材品牌，ctr中素材品牌数据异常"
        mail_msg = """
            <p>素材"""+str(creative_list)+"""中对应的素材品牌有多个，如果是分众自身素材，可以忽略</p> 
        """
        results_dict["9.监测一个素材是否对应多个素材品牌"] = "有素材对应多个素材品牌，ctr中素材品牌数据异常"
        send_email(mail_msg,mail_subject)
    else:
        mail_msg = """
            <p>素材对应一个素材品牌，ctr的素材品牌数据正常</p> 
        """
        results_dict["9.监测一个素材是否对应多个素材品牌"] = "素材对应一个素材品牌，ctr中素材品牌数据正常"
        #send_email(mail_msg)

    mail_msg = ""
    for key,values in results_dict.items():
        mail_msg = mail_msg + key + ": " + values + "<br>" 
    
    mail_msg = "<p>"+mail_msg+"</p>"
    mail_subject = "kuma排片数据同步到dw和ctr的监测报告"
    send_email(mail_msg,mail_subject)

# 程序运行入口
if __name__ == "__main__":
    main()

