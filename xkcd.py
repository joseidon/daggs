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
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
import json
from os import listdir
from os.path import isfile, join
import csvToJsonOperator
import pandas as pd

args = {
    'owner': 'airflow'
}
 #wip
hiveSQL_create_table_raw='''
CREATE EXTERNAL TABLE IF NOT EXISTS raw_data(
	month INT,
	num INT,
	year INT,
	safe_title STRING,
	alt STRING,
	title STRING,
    day INT,
) COMMENT 'raw DATA' PARTITIONED BY (year int) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t' STORED AS TEXTFILE LOCATION '/user/hadoop/raw/raw.txt'
TBLPROPERTIES ('skip.header.line.count'='1');
'''

hiveSQL_clean_table='''

'''

dag = DAG('xkcd3', default_args=args, description='xkcd practical exam',
          schedule_interval='56 18 * * *',
          start_date=datetime(2019, 10, 16), catchup=False, max_active_runs=1)

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

clear_local_import_dir = ClearDirectoryOperator(
    task_id='clear_import_dir',
    directory='/home/airflow/xkcd',
    pattern='*',
    dag=dag,
)

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
    directory='final',
    dag=dag,
)

clear_final_dir = ClearDirectoryOperator(
    task_id='clear_final_dir',
    directory='/home/airflow/final',
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
    directory='/user/hadoop/',
    hdfs_conn_id='raw',
    dag=dag,
)

upload_raw = HdfsPutFileOperator(
    task_id='upload_raw',
    local_file="/home/airflow/raw/raw.csv",
    remote_file='/user/hadoop/raw/raw.txt',
    hdfs_conn_id='hdfs',
    dag=dag,
)

create_raw_table = HiveOperator(
    task_id='create_raw_table',
    hql=hiveSQL_create_table_raw,
    hive_cli_conn_id='beeline',
    dag=dag)

clean_raw_table = HiveOperator(
    task_id='clean_raw_table',
    hql=hiveSQL_clean_table,
    hive_cli_conn_id='beeline',
    dag=dag)



#for i in range(int(Variable.get("number_of_latest_download")),int(Variable.get("number_of_comics"))):
#    general_xkcd_download = HttpDownloadOperator(
#        task_id='download_xdcd_' + str(i),
#        download_uri='https://xkcd.com/{}/info.0.json'.format(str(i)),
#        save_to='/home/airflow/xkcd/{}.json'.format(str(i)),
#        dag=dag,
#    )
#    general_xkcd_download.set_upstream(last_download_comic)
#    dummy_op.set_upstream(general_xkcd_download)








create_local_import_dir >> clear_local_import_dir >> create_local_import_dir_2 >> clear_local_import_dir_2 >> download_xkcd_latest >> last_comic >> last_download_comic
#last_comic >> tasks
dummy_op >> create_final_dir >> clear_final_dir >> csv_to_json >>create_hdfs_raw_dir >> upload_raw >> create_raw_table