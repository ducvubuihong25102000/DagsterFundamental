import pandas as pd
from dagster import IOManager, OutputContext, InputContext
from io import BytesIO
from minio import Minio

class MinIOIOManager(IOManager):
    def __init__(self, config):
        self._config = config
    
    def handle_output(self, context: OutputContext, obj: pd.DataFrame):
        # TODO: your code here
        # upload to MinIO <bucket_name>/<key_prefix>/<your_file_name>.csv
        print("handling output")
        client = Minio(
            "localhost:9000",
            access_key="minio",
            secret_key="minio123",
            secure=False
        )
        csv = obj.to_csv().encode('utf-8')
        client.put_object(
            "bronze",
            "ecom/test.csv",
            data=BytesIO(csv),
            length=len(csv),
            content_type='application/csv'
        )
        print("finish upload data to minio")
    
    def load_input(self, context: InputContext) -> pd.DataFrame:
        # TODO: your code here
        # download data <bucket_name>/<key_prefix>/<your_file_name>.csv from MinIO
        # read and return as Pandas Dataframe
        print("loading input")
        client = Minio(
            "localhost:9000",
            access_key="minio",
            secret_key="minio123",
            secure=False
        )
        
        obj = client.get_object(
            "bronze",
            "ecom/test.csv",
        )
        
        df = pd.read_csv(obj)

        return df