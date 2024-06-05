import time

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from dataset_utils import connect_s3_and_create_bucket, get_hourly_df, get_schema

SECONDS_BT_INSERT = 5
START_DATE = "06/06/2023"
END_DATE = "05/11/2024"

rbucket = "randata"
s3, fs = connect_s3_and_create_bucket(
    endpoint_url="http://localhost:9000",
    aws_access_key_id="catedra",
    aws_secret_access_key="catedrapass",
    rbucket=rbucket,
)

schema = get_schema()

for data_date in pd.date_range(start=START_DATE, end=END_DATE):
    df = pd.DataFrame()
    for hour in range(24):
        h_df = get_hourly_df(data_date, hour)
        df = pd.concat([df, h_df])

        parq = pa.Table.from_pandas(df, schema=schema)
        # Sobreescribe el parquet del dia hasta crear uno nuevo
        pq.write_to_dataset(
            parq,
            root_path=f"s3://{rbucket}/",
            basename_template="{i}.parquet",
            partition_cols=["month", "day"],
            filesystem=fs,
        )
        print(f"Wrote {data_date.date()} {hour}")
        time.sleep(SECONDS_BT_INSERT)
