from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults
import pandas

class csvToJsonOperator(BaseOperator):

    @apply_defaults
    def __init__(
            self,
            name: str,
            *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.name = name

    def execute(self, context):
        mypath = '/home/airflow/xkcd/'
        df = dataframe
        latest_download = 1
        onlyfiles = [f for f in listdir(mypath) if isfile(join(mypath, f))]
        for f in onlyfiles:
            with open('{}{}'.format(mypath,f)) as json_file:
                pandas.read_json(json_file)
                df.to_csv("{}/final/".format(mypath))

        df.dump
        return message