import os
from pathlib import Path
import pandas as pd

import configparser
from google.cloud import bigquery
from google.cloud import storage


class DripConnector:
    def __init__(self):
        self.config = configparser.RawConfigParser()
        # self.config.read('qplix-config.ini')
        # self.folder_path = Path(self.config['Path']['Dump Folder'])
        self.os = os
        self.os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = "third-being-207111-80cecaa4b7b0.json"
        self.client = bigquery.Client()

    def run_query(self, sql_string):
        """This function runs a query on the bigquery client and returns a dataframe"""
        QUERY = (sql_string)
        query_job = self.client.query(QUERY)
        rows = query_job.result()
        result_df = query_job.to_dataframe()
        return result_df

    def get_transactions(self):
        """This function gets all transactions from the bigquery database"""
        sql_query = '''
        SELECT 
            trans.*, port.qplix_portfolio_id
        FROM 
            `third-being-207111.ANALYTICS.analytics_transaction` AS trans
        LEFT JOIN (
            SELECT DISTINCT portfolio_id, qplix_portfolio_id
            FROM `third-being-207111.ANALYTICS.analytics_portfolio`
            WHERE qplix_portfolio_id IS NOT NULL
        ) AS port
        ON trans.portfolio_id = port.portfolio_id
        WHERE portfolio_drill_2 = "Wealth" AND qplix_portfolio_id IS NOT NULL 
        '''

        # Now you can use the `sql_query` string in your code.

        transactions_df = self.run_query(sql_query)
        return transactions_df


# main function to run the class
if __name__ == "__main__":
    drip = DripConnector()
    transactions_df = drip.get_transactions()


    folder_path = Path("/Users/anh-truc.lam/Library/CloudStorage/GoogleDrive-anh-truc.lam@liqid.de/My Drive/0005 AdHoc/2023-11-22_transaction-nachreich")
    transactions_df.to_csv(folder_path / "transactions_2.csv")
    # print(transactions_df.head())
    #
    # # find all unique portfolio_ids
    # portfolio_ids = transactions_df["portfolio_id"].unique()
    # # to df and to csv
    # portfolio_ids_df = pd.DataFrame(portfolio_ids)
    # portfolio_ids_df.to_csv(folder_path / "portfolio_ids.csv")

