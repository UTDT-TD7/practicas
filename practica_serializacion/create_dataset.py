import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from dataset_utils import connect_s3_and_create_bucket, get_hourly_df, get_schema

START_DATE = "05/28/2023"
END_DATE = "06/05/2023"

rbucket = "randata"
s3, fs = connect_s3_and_create_bucket(
    endpoint_url="http://localhost:9000",
    aws_access_key_id="catedra",
    aws_secret_access_key="catedrapass",
    rbucket=rbucket,
)

schema = get_schema()

df = pd.DataFrame()
for data_date in pd.date_range(start=START_DATE, end=END_DATE):
    for hour in range(24):
        h_df = get_hourly_df(data_date, hour)
        df = pd.concat([df, h_df])

parq = pa.Table.from_pandas(df, schema=schema)
pq.write_to_dataset(
    parq,
    root_path=f"s3://{rbucket}/",
    basename_template="{i}.parquet",
    partition_cols=["month", "day"],
    filesystem=fs,
)
