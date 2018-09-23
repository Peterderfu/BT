#https://www.kaggle.com/bigquery/bitcoin-blockchain
import os
import pandas as pd
from google.cloud.bigquery.client import Client
from datetime import datetime,timezone
# from bq_helper import BigQueryHelper
SERVICE_ACCOUNT_JSON = "file/Bitcoin-Tracing-472072344e9c.json"
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = SERVICE_ACCOUNT_JSON
client = Client()
query1 = """
#standardSQL
SELECT
  o.day,
  COUNT(DISTINCT(o.output_key)) AS recipients
FROM (
  SELECT
    TIMESTAMP_MILLIS((timestamp - MOD(timestamp,
          86400000))) AS day,
    output.output_pubkey_base58 AS output_key
  FROM
    `bigquery-public-data.bitcoin_blockchain.transactions`,
    UNNEST(outputs) AS output ) AS o
GROUP BY
  day
ORDER BY
  day
"""
# query_job = client.query(query)
# 
# iterator = query_job.result(timeout=3000)
# rows = list(iterator)
# 
# # Transform the rows into a nice pandas dataframe
# transactions = pd.DataFrame(data=[list(x.values()) for x in rows], columns=list(rows[0].keys()))
# 
# # Look at the first 10 headlines
# print(transactions.tail(10))
def list_field(dataset):
#list all the fields and sub-fields in dataset
    hn_dataset_ref = client.dataset(dataset[1], project=dataset[0])
    hn_dset = client.get_dataset(hn_dataset_ref)
    for t in client.list_tables(hn_dset):
        hn_full = client.get_table(hn_dset.table(t.table_id))
        for f1 in hn_full.schema:
            if (f1.fields):
                for f2 in f1.fields:
                    print("".join([t.table_id, ":",f1.name, "[",f1.field_type,"]",":",f2.name, "[",f2.field_type,"]"]))
            else:
                print("".join([t.table_id, ":",f1.name, "[",f1.field_type,"]"]))


if __name__ == "__main__":
#     list_field(['bigquery-public-data', 'bitcoin_blockchain'])
#     exit
    date_str = "2009-12-31"
    dt = datetime.strptime(date_str, "%Y-%m-%d")
    timestamp = int(dt.replace(tzinfo=timezone.utc).timestamp())*1000
    
    query = """
    SELECT
        timestamp,outputs
    FROM
        `bigquery-public-data.bitcoin_blockchain.transactions`
    WHERE
        timestamp < """ + str(timestamp) + """
    ORDER BY
       timestamp
    """
    print("Performing query : ")
    print(query)
    query_job = client.query(query)
 
    iterator = query_job.result(timeout=3000)
    rows = list(iterator)
    transactions = pd.DataFrame(data=[list(x.values()) for x in rows], columns=list(rows[0].keys()))
    transactions.head(10)
    assert(1==1)
    
