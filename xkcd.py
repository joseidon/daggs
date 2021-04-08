from datetime import datetime
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow.operators.http_download_operations import HttpDownloadOperator
from airflow.operators.zip_file_operations import UnzipFileOperator
from airflow.operators.hdfs_operations import HdfsPutFileOperator, HdfsGetFileOperator, HdfsMkdirFileOperator
from airflow.operators.filesystem_operations import CreateDirectoryOperator
from airflow.operators.filesystem_operations import ClearDirectoryOperator
from airflow.operators.hive_operator import HiveOperator
from airflow.operators.mysql_operator import MOperator
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from airflow.operators.hive_to_mysql as HiveToMySQL
import json
from os import listdir
from os.path import isfile, join
import csvToJsonOperator
import pandas as pd

args = {
    'owner': 'airflow'
}
#year INT,
cleanse_table='''
DROP TABLE IF EXISTS raw_data
'''


hiveSQL_create_table_raw='''
CREATE EXTERNAL TABLE raw_data(
    index INT,
	month INT,
	num INT,	
	safe_title STRING,
    transcript STRING,
	alt STRING,
    img STRING,
	title STRING,
    day INT,
    years INT
)ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t' STORED AS TEXTFILE LOCATION '/user/hadoop/raw'
TBLPROPERTIES ('skip.header.line.count'='1');
'''

hiveQL_create_partitioned='''
CREATE TABLE IF NOT EXISTS data (
    month INT,
	num INT,
	safe_title STRING,
    transcript STRING,
	alt STRING,
	title STRING,
    day INT
) PARTITIONED BY(year)STORED AS TEXTFILE LOCATION '/user/hadoop/raw';
'''

hiveSQL_partitioned = '''
INSERT OVERWRITE TABLE data
SELECT
    m.month,
    m.INT,
    m.safe_title,
    m.transcript,
    m.alt,
    m.title
    m.day
FROM
    raw_data m
    JOIN title_ratings r ON (m.tconst = r.tconst)
WHERE
    m.partition_year = {{ macros.ds_format(ds, "%Y-%m-%d", "%Y")}} and m.partition_month = {{ macros.ds_format(ds, "%Y-%m-%d", "%m")}} and m.partition_day = {{ macros.ds_format(ds, "%Y-%m-%d", "%d")}}
    AND r.partition_year = {{ macros.ds_format(ds, "%Y-%m-%d", "%Y")}} and r.partition_month = {{ macros.ds_format(ds, "%Y-%m-%d", "%m")}} and r.partition_day = {{ macros.ds_format(ds, "%Y-%m-%d", "%d")}}
    AND r.num_votes > 200000 AND r.average_rating > 8.6
    AND m.title_type = 'movie' AND m.start_year > 2000
'''

dag = DAG('xkcd3', default_args=args, description='xkcd practical exam',
          schedule_interval='56 18 * * *',
          start_date=datetime(2019, 10, 16), catchup=False, max_active_runs=1)

#Variable.set("number_of_latest_download", 0)
#Variable.set("number_of_comics", 1)

def get_number():
    number_of_comics = 0
    with open('/home/airflow/xkcd2/latest_xkcd.json') as json_file:
        data = json.load(json_file)
        number_of_comics = data['num']
    print(number_of_comics)
    #Variable.set("number_of_comics", number_of_comics)
    Variable.set("number_of_comics", 10)
    return 10
    #return number_of_comics

def get_download_number():
    maxVal = int(Variable.get("number_of_comics"))
    mypath = '/home/airflow/xkcd/'
    latest_download = 1
    onlyfiles = [f for f in listdir(mypath) if isfile(join(mypath, f))]
    for f in onlyfiles:
        with open('{}{}'.format(mypath,f)) as json_file:
            data = json.load(json_file)
            print(data['num'])
            if data['num']>latest_download & data['num']!= maxVal:
                latest_download = data['num']
    if latest_download == 404:
        latest_download = 405
    Variable.set("number_of_latest_download", latest_download)
    #Variable.set("number_of_latest_download", 10)
    return latest_download
    #return 10




create_local_import_dir = CreateDirectoryOperator(
    task_id='create_import_dir',
    path='/home/airflow',
    directory='xkcd',
    dag=dag,
)

#clear_local_import_dir = ClearDirectoryOperator(
#    task_id='clear_import_dir',
#    directory='/home/airflow/xkcd',
#    pattern='*',
#   dag=dag,
#)

create_local_import_dir_2 = CreateDirectoryOperator(
    task_id='create_import_dir_2',
    path='/home/airflow',
    directory='xkcd2',
    dag=dag,
)

clear_local_import_dir_2 = ClearDirectoryOperator(
    task_id='clear_import_dir_2',
    directory='/home/airflow/xkcd2',
    pattern='*',
    dag=dag,
)

create_final_dir = CreateDirectoryOperator(
    task_id='create_final_dir',
    path='/home/airflow',
    directory='raw',
    dag=dag,
)

clear_final_dir = ClearDirectoryOperator(
    task_id='clear_final_dir',
    directory='/home/airflow/raw',
    pattern='*',
    dag=dag,
)

download_xkcd_latest = HttpDownloadOperator(
    task_id='download_xkcd_latest',
    download_uri='https://xkcd.com//info.0.json',
    save_to='/home/airflow/xkcd2/latest_xkcd.json',
    dag=dag,
)


last_comic = PythonOperator(
    task_id='last_comic',
    python_callable=get_number,
    dag=dag)

last_download_comic = PythonOperator(
    task_id='last_download_comic',
    python_callable=get_download_number,
    dag=dag)

dummy_op = DummyOperator(
    task_id='dummy_op', 
    dag=dag)

csv_to_json = csvToJsonOperator.csvToJsonOperator(
    task_id='csv_to_json',
    dag=dag)

create_hdfs_raw_dir = HdfsMkdirFileOperator(
    task_id='mkdir_hdfs_raw_dir',
    directory='/user/hadoop/raw',
    hdfs_conn_id='hdfs',
    dag=dag,
)

upload_raw = HdfsPutFileOperator(
    task_id='upload_raw',
    local_file="/home/airflow/raw/raw.tsv",
    remote_file='/user/hadoop/raw/raw.tsv',
    hdfs_conn_id='hdfs',
    dag=dag,
)

create_raw_table = HiveOperator(
    task_id='create_raw_table',
    hql=hiveSQL_create_table_raw,
    hive_cli_conn_id='beeline',
    dag=dag)


cleanse_hive_table = HiveOperator(
    task_id='cleanse_hive_table',
    hql=cleanse_table,
    hive_cli_conn_id='beeline',
    dag=dag
)

to_mysql = HiveToMySqlOperator(
    task_id='to_mysql',

    dag=dag
)





for i in range(int(Variable.get("number_of_latest_download")),int(Variable.get("number_of_comics"))):
    general_xkcd_download = HttpDownloadOperator(
        task_id='download_xdcd_' + str(i),
        download_uri='https://xkcd.com/{}/info.0.json'.format(str(i)),
        save_to='/home/airflow/xkcd/{}.json'.format(str(i)),
        dag=dag,
    )
    general_xkcd_download.set_upstream(last_download_comic)
    dummy_op.set_upstream(general_xkcd_download)







#clear_local_import_dir >>
create_local_import_dir >>  create_local_import_dir_2 >> clear_local_import_dir_2 >> download_xkcd_latest >> last_comic >> last_download_comic
#last_comic >> tasks
dummy_op >> create_final_dir >> clear_final_dir >> csv_to_json >>create_hdfs_raw_dir >> upload_raw >> cleanse_hive_table>> create_raw_table >> to_mysql