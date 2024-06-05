import pandas as pd
import pyarrow.parquet as pq
import pyarrow as pa
import boto3
import numpy as np
import s3fs

s3 = boto3.client('s3',
                  endpoint_url='http://localhost:9000',
                  aws_access_key_id='catedra',
                  aws_secret_access_key='catedrapass')
fs = s3fs.S3FileSystem(anon=False, endpoint_url="http://localhost:9000",
                 key='catedra', secret='catedrapass', use_ssl=False)

rbucket = 'randata'
if rbucket not in [b["Name"] for b in s3.list_buckets()['Buckets']]:
    s3.create_bucket(Bucket=rbucket)

schema = pa.schema([
    ("ts", pa.date32()),
    ("cantidad", pa.int32()),
    ("month", pa.int32()),
    ("day", pa.int32()),
    ("hour", pa.int32()),
    ("valor", pa.float32()),
    ("tipo", pa.string())
])

df = pd.DataFrame()
for data_date in pd.date_range(start='05/28/2023', end='06/05/2023'):
    for hour in range(24):
        samples = np.random.randint(100,200)
        h_df = pd.DataFrame({
            "ts": data_date,
            "day": data_date.day,
            "month": data_date.month,
            "hour": hour,
            "cantidad": np.random.randint(0, 100, samples),
            "valor": np.random.rand(samples),
            "tipo": np.random.choice(["aaa", "bbb", "ccc", "ddd"], samples)
        })
        df = pd.concat([df,h_df])

parq = pa.Table.from_pandas(df, schema=schema)
pq.write_to_dataset(parq, root_path=f's3://{rbucket}/', basename_template="{i}.parquet",
                    partition_cols=['month', 'day'], filesystem=fs)
