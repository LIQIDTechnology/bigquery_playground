import os
import sys
import json
import gzip
import pathlib as Path
import pandas as pd
import datetime as dt
import configparser
from google.cloud import bigquery
from google.cloud import storage

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'third-being-207111-80cecaa4b7b0.json'


# storage_client = storage.Client()
# # Specify the bucket name
# bucket_name = 'liqid-airflow'
# # Specify the folder (prefix) you want to list (e.g., 'allocation data/')
# folder_prefix = 'allocation_data/'
# # Get a reference to the bucket
# bucket = storage_client.get_bucket(bucket_name)
# # List objects in the folder
# blobs = bucket.list_blobs(prefix=folder_prefix)
# # Print the object names within the folder
#
# for blob in blobs:
#     print(blob.name)
#     # read blob which is a json
#     # Download the compressed content as bytes
#
#     compressed_content = blob.download_as_bytes()
#
#     # Decompress the content
#
#     decompressed_content = gzip.decompress(compressed_content)
#
#     # Decode the decompressed content as UTF-8 text and parse as JSON
#     json_data = json.loads(decompressed_content.decode('utf-8'))
#     for k in json_data["resultLine"]["subLines"]:
#         print(k["name"], k["values"][0]["rawValue"])
#     print(json_data["resultLine"]["name"])
#     for k in json_data["resultLine"]:
#         print(k)
#
#     for x in json_data[12]:
#         print(x("Portfolio__c"))
#
#     pf_id = json_data[12]["Portfolio__c"]
#     # convert to dictionairy
#     pf_alloc_json = json.loads(json_data[12]["JSON_Payload__c"])
#     for key in pf_alloc_json:
#         print(key)
#     print(pf_alloc_json["headers"])
#
#     print(len(pf_alloc_json["resultLine"]["subLines"]))
#     for row in pf_alloc_json["resultLine"]["subLines"][3]["subLines"][0]["subLines"][0]["values"]:
#         print(row)
#
#     for key in pf_alloc_json["resultLine"]["subLines"]:
#         print(key["name"], key)
#     print(type(pf_alloc_json["resultLine"]["subLines"]))
#     print(pf_alloc_json["resultLine"]["values"])
#     for k in pf_alloc_json["resultLine"]["subLines"][3]:
#         print(k)
#     name = pf_alloc_json["resultLine"]["subLines"][3]["name"]
#     value = pf_alloc_json["resultLine"]["subLines"][3]["values"][0]["rawValue"]
#
#     pf_alloc_df = pf_alloc_json["resultLine"]["subLines"]
#
#
#     # print dictionairy pretty
#
#
# # convert to dataframe
# df = pd.DataFrame(json_data)
#
# # Convert the data to a DataFrame






# Perform a query.
client = bigquery.Client()




#
# QUERY = (
#     'SELECT * '
#     'FROM `third-being-207111.ANALYTICS.analytics_transaction` LIMIT 1000')


QUERY = (
    'SELECT *'
    'FROM `third-being-207111.DWH.dwh_salesforce_twr`'
    'WHERE dt = "2021-05-10" ')



query_job = client.query(QUERY)  # API request
rows = query_job.result()  # Waits for query to finish
df = query_job.to_dataframe()
print(df)

# write this code as class so in can be imported
# class BigQueryConnector:
#     def __init__(self):
#         self.client = bigquery.Client()
#
#     def sample_query(self, sql_string):
#         QUERY = (sql_string)
#         query_job = self.client.query(QUERY)  # API request
#         rows = query_job.result()  # Waits for query to finish
#         # create dataframe from query_job.result()
#         df = query_job.to_dataframe()
#         return df
#
#
# # main function
# if __name__ == '__main__':
#     bq = BigQueryConnector()
#     sql_string = 'SELECT * FROM `third-being-207111.ANALYTICS.analytics_nav` LIMIT 1000'
#     df = bq.sample_query(sql_string)