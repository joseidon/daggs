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
        df = pandas.DataFrame(columns = ["month", "num", 'safe_title', 'transcript', 'alt', 'img', 'title', 'day', 'years'])
        latest_download = 1
        onlyfiles = [f for f in listdir(mypath) if isfile(join(mypath, f))]
        data = []
        for f in onlyfiles:
            print('{}{}'.format(mypath,f))
            with open('{}{}'.format(mypath,f)) as json_file:
                #print("still alive")
                rj = pandas.read_json(json_file, typ = 'series', encoding='utf16')
                #dfs = pandas.DataFrame(columns = ["month", "num", 'link', 'year', 'news', 'safe_title', 'transscript', 'alt', 'img', 'title', 'day'])
                dfs = pandas.DataFrame.from_dict(rj)
                #print(dfs)
                
                dfs = dfs.transpose()
                #dfs.insert(11,"years",dfs["year"], True)
                dfs = dfs.drop(labels = ["month","link","news","year","day"], axis=1,)
                #dfs = dfs.insert(9,"years",dfs["year"], True)
                #df['years'] = df['year']
                #df.drop(columns=["year"])
                dfs = dfs.reset_index(drop=True)
                dfs =dfs.replace(r'\t',' ', regex=True) 
                dfs =dfs.replace(r'\n',' ', regex=True) 
                dfs["safe_title"] =str(dfs["safe_title"]).encode("ascii", "replace")
                dfs["transcript"] =str(dfs["transcript"]).encode("ascii", "replace")
                dfs["alt"] =str(dfs["alt"]).encode("ascii", "replace")
                dfs["title"] =str(dfs["title"]).encode("ascii", "replace")
                #print(dfs)
                data.append(dfs)
        print("Print List:")
        #print(data)df.append([1,2,3,4,5,6,7,8,9,0,11,12])
        df = pandas.concat(data, axis=0, ignore_index=True)

        #df.drop(labels = ["link","news"], axis=1,)
        #df.append(data)#,  ignore_index = True
        print(df)
        df.to_csv("/home/airflow/raw/raw.tsv", sep=';', header=False)
        