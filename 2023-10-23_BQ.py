import os
import sys
import json
import gzip
import numpy as np
import datetime as dt
from datetime import date as dt_date
import db_dtypes

from pathlib import Path
import pandas as pd

import configparser
from google.cloud import bigquery
from google.cloud import storage

class BigQueryConnector:
    def __init__(self):
        self.config = configparser.RawConfigParser()
        self.config.read('qplix-config.ini')
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = self.config['Credentials']['Goolge_credentials']
        self.client = bigquery.Client()

    def get_twr(self, portfolio_id, start_date=None, end_date=None):
        # Get the twr in period [start_date to end_date] for portfolio [portfolio_id],
        # if there is no start_date/end_date inputs, the return the twr time series.

        # works but risky!!!! f-Strings + SQL
        QUERY = (f"""
            SELECT *
            FROM `third-being-207111.DWH.dwh_salesforce_twr`
            WHERE portfolio_id = "{portfolio_id}" """)

        query_job = self.client.query(QUERY)  # API request
        df = query_job.to_dataframe()

        df['dt'] = df['dt'].apply(lambda x: dt.datetime.strptime(str(x)[:10], "%Y-%m-%d").date())

        # df['dt'] = pd.to_datetime(df['dt'])
        df.drop(columns=['portfolio_id'], inplace=True)
        df.set_index('dt', inplace=True)
        df.sort_index(ascending=False, inplace=True)

        if (start_date is not None) and (end_date is not None):
            twr_start = df.loc[start_date]['twr']
            twr_end = df.loc[end_date]['twr']
            return_of_period = ((twr_end + 1) / (twr_start + 1)) - 1
            return return_of_period

        if (start_date is None) and (end_date is None):
            return df

        else:
            raise TypeError("if there is no start_date/end_date inputs, the return the twr time series.")






    # def get_twr_time_series(portfolio_id):
    #     # get nav for a single portfolio with a single portfolio id
    #
    #     client = bigquery.Client()
    #     # works but risky!!!! f-Strings + SQL
    #
    #     QUERY = (f"""
    #         SELECT *
    #         FROM `third-being-207111.DWH.dwh_salesforce_twr`
    #         WHERE portfolio_id = "{portfolio_id}" """)
    #
    #     query_job = client.query(QUERY)  # API request
    #     df_twr = query_job.to_dataframe()
    #
    #     df_twr['dt'] = pd.to_datetime(df_twr['dt'])
    #     df_twr.drop(columns=['portfolio_id'], inplace=True)
    #     df_twr.set_index('dt', inplace=True)
    #     df_twr.sort_index(ascending=False, inplace=True)
    #
    #     return df_twr


    def get_nav(self, portfolio_id):
        # get nav for a single portfolio with a single portfolio id

        # works but risky!!!! f-Strings + SQL
        QUERY = (f"""
            SELECT *
            FROM `third-being-207111.DWH.dwh_salesforce_nav`
            WHERE portfolio_id = "{portfolio_id}" """)

        query_job = self.client.query(QUERY)  # API request
        df_nav = query_job.to_dataframe()

        df_nav['dt'] = pd.to_datetime(df_nav['dt'])
        df_nav.drop(columns=['portfolio_id'], inplace=True)
        df_nav.set_index('dt', inplace=True)
        df_nav.sort_index(ascending=False, inplace=True)

        return df_nav


    def get_multi_nav(self, portfolio_id_dict):
        # get nav for multi portfolio with multi portfolio id in dictionary

        df_multi_nav = pd.DataFrame()

        # Generate the SQL Commands using f-string
        # There is no "OR" before First id
        first_id = True
        for key, value in portfolio_id_dict.items():

            if first_id:
                sql_command_string = f"portfolio_id = \"{value}\""
                first_id = False
            else:
                sql_command_string = sql_command_string + f" OR portfolio_id = \"{value}\""

        # works but risky!!!! f-Strings + SQL
        QUERY = (f"""
            SELECT *
            FROM `third-being-207111.DWH.dwh_salesforce_nav`
            WHERE {sql_command_string} """)

        query_job = self.client.query(QUERY)  # API request
        df_nav = query_job.to_dataframe()
        df_nav['dt'] = pd.to_datetime(df_nav['dt'])
        df_nav.set_index('dt', inplace=True)
        df_nav.sort_index(ascending=False, inplace=True)

        for key, value in portfolio_id_dict.items():
            data = df_nav.loc[df_nav['portfolio_id'] == value].drop(columns="portfolio_id")
            data = data.rename(columns={'nav': key})
            df_multi_nav = pd.concat([df_multi_nav, data], axis=1, join='outer')

        return df_multi_nav


    def get_portfolio_info(self, portfolio_id):
        QUERY = (f"""
            SELECT *
            FROM `third-being-207111.ANALYTICS.analytics_portfolio` """)

        query_job = self.client.query(QUERY)  # API request
        df = query_job.to_dataframe()
        portfolio_info = df.loc[df['portfolio_id'] == portfolio_id]

        return portfolio_info


    def qplix_id_to_portfolio_id(self, qplix_id_dict):
        # find coressponding portfolio id to qplix id

        portfolio_id = {}
        portfolio_id_values = {}

        QUERY = (f"""
            SELECT *
            FROM `third-being-207111.ANALYTICS.analytics_portfolio` """)

        query_job = self.client.query(QUERY)  # API request
        df = query_job.to_dataframe()

        # Get the corresponding portfolio_id from qplix_portfolio_id
        for key, value in qplix_id_dict.items():

            portfolio_id[key] = df.loc[df['qplix_portfolio_id'] == value]['portfolio_id'].values

            if portfolio_id[key].size > 0:
                portfolio_id_values[key] = portfolio_id[key][0]

        return portfolio_id_values


    def portfolio_id_to_qplix_id(self, portfolio_id_dict):
        # find coressponding portfolio id to qplix id

        qplix_id = {}
        qplix_id_values = {}

        QUERY = (f"""
            SELECT *
            FROM `third-being-207111.ANALYTICS.analytics_portfolio` """)

        query_job = self.client.query(QUERY)  # API request
        df = query_job.to_dataframe()

        # Get the corresponding portfolio_id from qplix_portfolio_id
        for key, value in portfolio_id_dict.items():

            qplix_id[key] = df.loc[df['portfolio_id'] == value]['qplix_portfolio_id'].values

            if qplix_id[key].size > 0:
                qplix_id_values[key] = qplix_id[key][0]

        return qplix_id_values


    def get_performance(self, qplix_id_dict):
        performance = {}

        # Get the corresponding portfolio_id from qplix_portfolio_id
        performance_dict_portfolio_id = BigQueryConnector.qplix_id_to_portfolio_id(qplix_id_dict)

        for key, value in performance_dict_portfolio_id.items():
            performance[key] = BigQueryConnector.get_nav(self, value)

        return performance


    def get_multi_performance(self, qplix_id_dict):
        # Get the corresponding portfolio_id from qplix_portfolio_id
        performance_dict_portfolio_id = BigQueryConnector.qplix_id_to_portfolio_id(qplix_id_dict)

        multi_performance = BigQueryConnector.get_multi_nav(performance_dict_portfolio_id)

        return multi_performance


    def BQ_download_performance(self, qplix_id_dict):
        # Get the corresponding portfolio_id from qplix_portfolio_id
        performance_dict_portfolio_id = BigQueryConnector.qplix_id_to_portfolio_id(qplix_id_dict)

        performance_df = pd.DataFrame()

        for key, value in performance_dict_portfolio_id.items():
            data = BigQueryConnector.get_nav(self, value)
            data = data.rename(columns={'nav': key})
            performance_df = pd.concat([performance_df, data], axis=1)

        return performance_df


# main function
if __name__ == '__main__':


    portfolio_id = "a000Y000019ZlfSQAS"

    # portfolio_id = {
    #     1: "a007R000012jC78QAE",
    #     2: "a007R000012jIyyQAE",
    #     3: "a007R000014HHSoQAO",
    #     4: "a007R000012jAG1QAM",
    #     5: "a007R000012iwWJQAY",
    #     6: "a007R000012ihpEQAQ",
    #     7: "a007R000012ifTiQAI",
    #     8: "a007R000012idbRQAQ",
    #     9: "a007R000012ia6bQAA",
    #     10: "a007R000012iTimQAE",
    #     11: "a007R000012iRavQAE"}

    # portfolio_id_to_qplix_id(portfolio_id)

    BQ = BigQueryConnector()

    start_date = dt.date(2021, 5, 10)
    end_date = dt.date(2023, 5, 10)

    return_of_period = BQ.get_twr(portfolio_id, start_date, end_date)
    # nav = get_nav(portfolio_id)
    twr_time_series = BQ.get_twr(portfolio_id)

    # portfolio_info = get_portfolio_info(portfolio_id)
    # Strategy = portfolio_info["portfolio_drill_1"].iloc[0]
    # Risk_class = portfolio_info["portfolio_risk_level"].iloc[0]

    # qplix_id_dict = {strat: config['Client ID'][strat] for strat in config['Client ID']}
    # performance_report = get_performance(qplix_id_dict)

    # 28.5s
    # BQ_performance = BQ_download_performance(qplix_id_dict)

    # 7.68s
    # BQ_performance = get_multi_performance(qplix_id_dict)

    print("return_of_period", return_of_period)
    print("twr_time_series", twr_time_series)
    print("twr_time_series", twr_time_series)
