# Uploadoss
Uploadoss is the general module of uploading database data and local files to the Alibaba Cloud OSS 


## Installation
download uploadoss-0.1.tar.gz
https://pypi.org/project/uploadoss/#files
pip install uploadoss-0.1.tar.gz

## Usage
Some simple things you can do with uploadoss

```
#upload local file(Logs, CSV, and their gzip compression) to Alibaba Cloud OSS 

from uploadoss import uploadoss
conf_path = 'C:\demo\oss\conf.ini'
bucket = uploadoss.getossbucket(conf_path)
# Directory of local files
local_path = r'C:\\demo\\data\\file\\'
# The file name in the local directory, also used as the file name on the OSS side
file_name = 'test1.log.gz'
# OSS Directory
oss_path = 'xxx/testlocalfile/'
result = uploadoss.file_to_oss(bucket,local_path = local_path,file_name = file_name,oss_path = oss_path)
print('http status: {0}'.format(result.status))

# Through a single SQL statement

conn = uploadoss.getmysqlconn(conf_path=conf_path)
# 
sql_text = 'select * from bigdata_demo.event_info limit 1' 
# The file where the SQL query results are stored, also serving as the file name on the OSS side
file_name = 'sql_text.csv.gz'
# OSS Directory
oss_path = 'xxx/testsqltext/dt=20230926/'
result = uploadoss.mysql_to_oss_table(conn=conn,bucket=bucket,local_path=local_path,file_name=file_name,sql_text=sql_text,oss_path = oss_path)
print('http status: {0}'.format(result.status))


# Through a multiple SQL statements,The separator is \001 ,e.g. event_info\001SQL_TEXT
table_conf_list=r'C:\\demo\\oss\\table_config.txt'
result = uploadoss.mysql_to_oss_list(conf_path = conf_path,local_path = local_path,oss_dire_path = oss_dire_path,table_list_conf = table_conf_list )

```
