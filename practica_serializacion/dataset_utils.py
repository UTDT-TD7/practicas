from typing import Tuple

import boto3
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import s3fs


def connect_s3_and_create_bucket(
    endpoint_url: str, aws_access_key_id: str, aws_secret_access_key: str, rbucket: str
) -> Tuple[boto3.resources.base.ServiceResource, s3fs.S3FileSystem]:
    s3 = boto3.client(
        "s3",
        endpoint_url=endpoint_url,
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
    )
    fs = s3fs.S3FileSystem(
        anon=False,
        endpoint_url=endpoint_url,
        key=aws_access_key_id,
        secret=aws_secret_access_key,
        use_ssl=False,
    )

    if rbucket not in [b["Name"] for b in s3.list_buckets()["Buckets"]]:
        s3.create_bucket(Bucket=rbucket)
    return s3, fs


def get_schema() -> pa.schema:
    return pa.schema(
        [
            ("ts", pa.date32()),
            ("cantidad", pa.int32()),
            ("month", pa.int32()),
            ("day", pa.int32()),
            ("hour", pa.int32()),
            ("valor", pa.float32()),
            ("tipo", pa.string()),
        ]
    )


def get_hourly_df(data_date: pd.Timestamp, hour: int) -> pd.DataFrame:
    samples = np.random.randint(100, 200)
    return pd.DataFrame(
        {
            "ts": data_date,
            "day": data_date.day,
            "month": data_date.month,
            "hour": hour,
            "cantidad": np.random.randint(0, 100, samples),
            "valor": np.random.rand(samples),
            "tipo": np.random.choice(["aaa", "bbb", "ccc", "ddd"], samples),
        }
    )
