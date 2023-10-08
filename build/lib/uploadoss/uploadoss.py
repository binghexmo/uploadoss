# -*- coding: utf-8 -*-
import oss2
import pymysql
import pandas as pd
from configparser import ConfigParser
import os

def mysql_to_oss_list(conf_path,local_path,oss_dire_path,table_list_conf):
    config = ConfigParser() 
    config.read(conf_path)  
    # OSS config
    OSS_ACCESS_KEY_ID = config['oss']['OSS_ACCESS_KEY_ID']
    OSS_ACCESS_KEY_SECRET = config['oss']['OSS_ACCESS_KEY_SECRET']
    bucket = config.get('oss', 'bucket')
    endpoint = config.get('oss', 'endpoint')

    # mysql config 
    host = config['mysql']['host']
    user = config['mysql']['user']
    password = config['mysql']['password']
    port = int(config['mysql']['port'])
    database = config['mysql']['database']
    charset = config['mysql']['charset']

    conn = pymysql.connect(host=host, port=port, user=user, password=password, database=database,charset=charset)

    auth = oss2.Auth(OSS_ACCESS_KEY_ID, OSS_ACCESS_KEY_SECRET)
    
    bucket = oss2.Bucket(auth, endpoint, bucket)

    local_file = "" 

    if not os.path.exists(local_path):
        os.mkdir(local_path) 

    with open (table_list_conf,'r') as f:

        data  = f.readlines()

    for line in data:

        try:  
            table_name,sql_text = line.strip().split("\\001")
            if len(sql_text) <2 :
                sql_text = "select * from "+table_name
            file_name = table_name + ".csv.gz"
            local_file = local_path+file_name
            df = pd.read_sql(sql_text,conn)
            df.to_csv(local_file,index = False,compression='gzip')

            # Create OSS directory
            oss_path = oss_dire_path+table_name+"/"
            bucket.put_object(oss_path, '') 

            result = oss_upload_file(local_file,oss_path,file_name,bucket)

            print(sql_text)
            # HTTP返回码。
            print('http status: {0}'.format(result.status))
            # 请求ID。请求ID是本次请求的唯一标识，强烈建议在程序日志中添加此参数。
            print('request_id: {0}'.format(result.request_id))
            # ETag是put_object方法返回值特有的属性，用于标识一个Object的内容。
            print('ETag: {0}'.format(result.etag))
            # HTTP响应头部。
            print('date: {0}'.format(result.headers['date']))
        except Exception as e:  
            print("An error occurred:", e)  
            continue  
        finally:  
            pass
    conn.close()
    #return result   

def oss_upload_file(local_file,oss_path,file_name,bucket):
    
    with open(local_file,'rb') as file_obj:
        result = bucket.put_object(oss_path+file_name, file_obj)
    return result

def file_to_oss(bucket,local_path,file_name,oss_path):

    local_file = local_path + file_name
    # Create OSS directory
    bucket.put_object(oss_path, '') 

    result = oss_upload_file(local_file,oss_path,file_name,bucket)
    return result 


# Get OSS Bucket
def getossbucket(conf_path):

    config = ConfigParser()  
    config.read(conf_path)  
    # OSS config
    OSS_ACCESS_KEY_ID = config['oss']['OSS_ACCESS_KEY_ID']
    OSS_ACCESS_KEY_SECRET = config['oss']['OSS_ACCESS_KEY_SECRET']
    bucket = config.get('oss', 'bucket')
    endpoint = config.get('oss', 'endpoint')

    auth = oss2.Auth(OSS_ACCESS_KEY_ID, OSS_ACCESS_KEY_SECRET)
    
    bucket = oss2.Bucket(auth, endpoint, bucket)

    return bucket

def getmysqlconn(conf_path):
    config = ConfigParser()  
    config.read(conf_path) 
    # mysql config 
    host = config['mysql']['host']
    user = config['mysql']['user']
    password = config['mysql']['password']
    port = int(config['mysql']['port'])
    database = config['mysql']['database']
    charset = config['mysql']['charset']

    conn = pymysql.connect(host=host, port=port, user=user, password=password, database=database,charset=charset)
    return conn

# 数据库单次操作
def mysql_to_oss_table(conn,bucket,local_path,file_name,sql_text,oss_path):

    df = pd.read_sql(sql_text,conn) 

    print(sql_text)

    if not os.path.exists(local_path):
        os.mkdir(local_path) 

    df.to_csv(local_path+file_name,index = False,compression='gzip')

    # 上传文件。
    # 如果需要在上传文件时设置文件存储类型（x-oss-storage-class）和访问权限（x-oss-object-acl），请在put_object中设置相关Header。
    # headers = dict()
    # headers["x-oss-storage-class"] = "Standard"
    # headers["x-oss-object-acl"] = oss2.OBJECT_ACL_PRIVATE
    # 填写Object完整路径和字符串。Object完整路径中不能包含Bucket名称。
    # result = bucket.put_object('exampleobject.txt', 'Hello OSS', headers=headers)


    local_file = local_path + file_name
    # Create OSS directory 
    bucket.put_object(oss_path, '') 

    result = oss_upload_file(local_file,oss_path,file_name,bucket)

    return result

if __name__ == '__main__':

    from datetime import datetime, timedelta
    today = datetime.today()
    yesterday = today - timedelta(days=1)
    # 日分区表目录
    dir_name = 'dt=' + yesterday.strftime('%Y%m%d') + '/' 
    oss_dire_path = 'bigdata_demo_gz/'

    oss_path =  oss_dire_path + dir_name
    print(dir_name)
    conf_path = '' 
    conf_path = 'C:\demo\oss\conf.ini'
    local_path = r'C:\\demo\\data\\'
    csv_name = 'bigdata_demo_file.csv'

    # 配置多个表及SQL语句,分隔符要求 \001
    
    table_conf_list=r'C:\\demo\\oss\\table_config.txt'
    mysql_to_oss_list(conf_path = conf_path,local_path = local_path,oss_dire_path = oss_dire_path,table_list_conf = table_conf_list )

    # 单个文件上传到OSS
    bucket = getossbucket(conf_path) 
    # result = file_to_oss(bucket,local_path = local_path,file_name = csv_name,oss_path = oss_path)

    # # HTTP返回码。
    # print('http status: {0}'.format(result.status))
    # # 请求ID。请求ID是本次请求的唯一标识，强烈建议在程序日志中添加此参数。
    # print('request_id: {0}'.format(result.request_id))
    # # ETag是put_object方法返回值特有的属性，用于标识一个Object的内容。
    # print('ETag: {0}'.format(result.etag))
    # # HTTP响应头部。
    # print('date: {0}'.format(result.headers['date']))

    #单个sql
    conn = getmysqlconn(conf_path=conf_path)
    sql_text = 'select * from bigdata_demo.event_info limit 1' 
    csv_name = 'sql_text.csv.gz'
    print(oss_path)
    # 内部对CSV进行GZIP压缩处理，文件名格式.csv.gz
    result = mysql_to_oss_table(conn=conn,bucket=bucket,local_path=local_path,file_name=csv_name,sql_text=sql_text,oss_path = oss_path)
    # HTTP返回码。
    print('http status: {0}'.format(result.status))
    # 请求ID。请求ID是本次请求的唯一标识，强烈建议在程序日志中添加此参数。
    print('request_id: {0}'.format(result.request_id))
    # ETag是put_object方法返回值特有的属性，用于标识一个Object的内容。
    print('ETag: {0}'.format(result.etag))
    # HTTP响应头部。
    print('date: {0}'.format(result.headers['date']))





