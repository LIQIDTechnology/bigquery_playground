import os
import sys
import json
import gzip
from pathlib import Path
import pandas as pd
import datetime as dt
import configparser
from google.cloud import bigquery
from google.cloud import storage


class PerformanceAverage:

    def __init__(self):
        # Set up the environment variable for the google credentials
        self.os = os
        credentials_path = Path("third-being-207111-80cecaa4b7b0.json").absolute()
        self.os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = str(credentials_path)        # Set up a storage client for allocation data
        self.storage_client = storage.Client()
        # # Get a reference to the bucket
        # self.bucket = self.storage_client.get_bucket("liqid-airflow")
        # # List objects in the folder
        # self.blobs = self.bucket.list_blobs(prefix="allocation_data/")
        # # Set up a bigquery client
        # self.client = bigquery.Client()

    def run_query(self, sql_string):
        """This function runs a query on the bigquery client and returns a dataframe"""
        QUERY = (sql_string)
        query_job = self.client.query(QUERY)  # API request
        rows = query_job.result()  # Waits for query to finish
        result_df = query_job.to_dataframe()
        return result_df

    def blob_routine(self, result_df):
        blob_lst = []
        for blob in self.blobs:
            # if 20220701 in blob.name then append
            if "20231009" in blob.name:
                blob_lst.append(blob)
        for blob in blob_lst:
            # Blob UTF-8 text and parse as JSON # 250 clients in json_data
            compressed_content = blob.download_as_bytes()
            decompressed_content = gzip.decompress(compressed_content)
            json_data = json.loads(decompressed_content.decode('utf-8'))


            for client in json_data:
                client = json_data[12]
                client_id = client["Portfolio__c"]
                # Geldmarktvalue


            for k in json_data:
                print(k)


# main function to run the class
if __name__ == "__main__":
    pf_avg = PerformanceAverage()
    sql_string = "SELECT twr_tbl.*, nav_tbl.nav, " \
                 "pf_tbl.vbank_number, " \
                 "pf_tbl.portfolio_state, " \
                 "pf_tbl.portfolio_risk_level, " \
                 "pf_tbl.portfolio_drill_1, " \
                 "pf_tbl.portfolio_creation_dt, " \
                 "pf_tbl.qplix_portfolio_id " \
                 "FROM `third-being-207111.DWH.dwh_salesforce_twr` AS twr_tbl " \
                 "LEFT JOIN `third-being-207111.DWH.dwh_salesforce_nav` AS nav_tbl " \
                 "ON twr_tbl.portfolio_id = nav_tbl.portfolio_id AND twr_tbl.dt = nav_tbl.dt " \
                 "LEFT JOIN `third-being-207111.ANALYTICS.analytics_portfolio` AS pf_tbl " \
                 "ON twr_tbl.portfolio_id = pf_tbl.portfolio_id " \
                 "WHERE twr_tbl.twr IS NOT NULL " \
                 "AND nav_tbl.nav > 40000 " \
                 "AND DATE(portfolio_creation_dt) < DATE_SUB(twr_tbl.dt, INTERVAL 7 DAY) " \
                 "AND pf_tbl.portfolio_state = 'Invested' " \
                 "LIMIT 1000"

    result_df = pf_avg.run_query(sql_string)

    pf_avg.blob_routine(result_df)


