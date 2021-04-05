from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults
import pandas
import json
from os import listdir
from os.path import isfile, join

class csvToJsonOperator(BaseOperator):

    @apply_defaults
    def __init__(
            self,
           # task_id: str,
            *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        #self.task_id = task_id

    def execute(self, context):
        mypath = '/home/airflow/xkcd/'
        df = pandas.DataFrame(columns = ["month", "num", 'link', 'year', 'news', 'safe_title', 'transscript', 'alt', 'img', 'title', 'day'])
        latest_download = 1
        onlyfiles = [f for f in listdir(mypath) if isfile(join(mypath, f))]
        data = []
        for f in onlyfiles:
            with open('{}{}'.format(mypath,f)) as json_file:
                rj = pandas.read_json(json_file, typ = 'series')
                #dfs = pandas.DataFrame(columns = ["month", "num", 'link', 'year', 'news', 'safe_title', 'transscript', 'alt', 'img', 'title', 'day'])
                dfs = pandas.DataFrame.from_dict(rj)
                #print(dfs)
                dfs = dfs.transpose()
                print(dfs)
                data.append(dfs)
        print("Print List:")
        #print(data)df.append([1,2,3,4,5,6,7,8,9,0,11,12])
        df = pandas.concat(data, axis=1)
        #df.append(data)#,  ignore_index = True
        print(df)
        df.to_csv("/home/airflow/final/final.csv")
