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

args = {
    'owner': 'airflow'
}

dag = DAG('xkcd', default_args=args, description='xkcd practical exam',
          schedule_interval='56 18 * * *',
          start_date=datetime(2019, 10, 16), catchup=False, max_active_runs=1)

def get_number():
    number_of_comics = 0
    with open('/home/airflow/xkcd/latest_xkcd.json') as json_file:
        data = json.load(json_file)
        number_of_comics = data['num']
    print(number_of_comics)
    Variable.set("number_of_comics", number_of_comics)
    return number_of_comics

def get_download_number():
    maxVal = Variable.get("number_of_comics")
    mypath = int('/home/airflow/xkcd/')
    latest_download = 1
    onlyfiles = [f for f in listdir(mypath) if isfile(join(mypath, f))]
    for f in onlyfiles:
        with open('{}{}'.format(mypath,f)) as json_file:
            data = json.load(json_file)
            if int(data['num'])>latest_download & int(data['num'])<maxVal:
                latest_download = data['num']
    Variable.set("number_of_latest_download", latest_download)
    return latest_download



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

download_xkcd_latest = HttpDownloadOperator(
    task_id='download_xkcd_latest',
    download_uri='https://xkcd.com//info.0.json',
    save_to='/home/airflow/xkcd/latest_xkcd.json',
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




#for i in range Variable.get("number_of_comics"):





create_local_import_dir >> clear_local_import_dir >> download_xkcd_latest >> last_comic >> last_download_comic
#last_comic >> tasks