import os
from google.cloud import bigquery
import pandas as pd
client = bigquery.Client()
query = """
CREATE TABLE IF NOT EXISTS custom-sylph-458304-r4.peerisland.products_new
(product_id INT64, price INT64, name STRING,category STRING)
"""
job = client.query(query)
job.result()

df = pd.read_csv(r"C:\Users\Admin\Downloads\products_new.csv")

#transformation
df.drop_duplicates()

table_id = "custom-sylph-458304-r4.peerisland.products_new"

job_config = bigquery.LoadJobConfig(write_disposition = "WRITE_TRUNCATE", autodetect = True)

job = client.load_table_from_dataframe(df, table_id, job_config = job_config)
job.result()


