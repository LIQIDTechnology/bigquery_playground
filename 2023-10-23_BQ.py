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
        self.folder_path = Path(self.config['Path']['Nachreich Folder'])
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = self.config['Credentials']['Goolge_credentials']
        self.client = bigquery.Client()
        self.strategy_ls = ["LIQID Select", "LIQID Global Future", "LIQID Global"]
        self.risklevel_ls = [str(i) for i in range(10, 110, 10)]

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

    datetime_ls = [dt.date(2022, 7, 1) + dt.timedelta(days=i) for i in range(0, 488)]
    all_qplix_id_ls = []

    for t in datetime_ls:
        for strat in BQ.strategy_ls:
            for rc in BQ.risklevel_ls:
                # qplix_id_LIQID Global Future_10_2023-05-20
                qplix_id_df = pd.read_csv(BQ.folder_path / "id_list_ever" / f"qplix_id_{strat}_{rc}_{t}.csv")
                # convert to list
                qplix_id_ls = qplix_id_df["0"].tolist()
                all_qplix_id_ls.extend(qplix_id_ls)

    all_qplix_id_ls = list(set(all_qplix_id_ls))
    all_qplix_id_ls_str = ", ".join(map(lambda x: f"\"{x}\"", all_qplix_id_ls))
    where_clause = f'WHERE portfolio.qplix_portfolio_id IN ({all_qplix_id_ls_str})'
    sql_query = f"""
                    SELECT
                      wealth.*,
                      portfolio.qplix_portfolio_id,
                      salesforce_nav.nav
                    FROM
                      `third-being-207111.ANALYTICS.analytics_wealth_twr_daily` AS wealth
                    LEFT JOIN
                      `third-being-207111.ANALYTICS.analytics_portfolio` AS portfolio
                    ON
                      wealth.portfolio_id = portfolio.portfolio_id
                    LEFT JOIN
                      `third-being-207111.DWH.dwh_salesforce_nav` AS salesforce_nav
                    ON
                      wealth.portfolio_id = salesforce_nav.portfolio_id
                      AND wealth.dt = salesforce_nav.dt
                    {where_clause} AND wealth.dt >= "2022-07-01" AND wealth.dt <= "2023-10-31" AND salesforce_nav.nav >= 40000;
                    """

    test = BQ.client.query(sql_query)
    df = test.to_dataframe()

    count_df = pd.DataFrame()
    ret_df = pd.DataFrame()
    for t in datetime_ls:
        for strat in BQ.strategy_ls:
            for rc in BQ.risklevel_ls:
                # qplix_id_LIQID Global Future_10_2023-05-20
                qplix_id_df = pd.read_csv(BQ.folder_path / "id_list_ever" / f"qplix_id_{strat}_{rc}_{t}.csv")
                # convert to list
                qplix_id_ls = qplix_id_df["0"].tolist()

                # slice df on dt == t and qplix_id in qplix_id_ls
                tmp_df = df.loc[(df["dt"] == t) & (df["qplix_portfolio_id"].isin(qplix_id_ls))]

                tmp_df.to_csv(BQ.folder_path / "id_list_df" / f"qplix_id_{strat}_{rc}_{t}_performance.csv")

                twr_avg = tmp_df["twr"].mean()

                ret_df.loc[t, f"{strat}_{rc}"] = twr_avg
                count_df.loc[t, f"{strat}_{rc}"] = tmp_df.shape[0]

                print(f"{strat}_{rc}_{t} is done")

    ret_df.to_csv(BQ.folder_path / "FINAL_2_ret_df.csv")
    count_df.to_csv(BQ.folder_path / "FINAL_2_count_df.csv")