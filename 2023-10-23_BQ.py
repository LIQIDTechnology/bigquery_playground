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


def get_twr(portfolio_id, start_date, end_date):

    client = bigquery.Client()
    start_date = start_date.strftime('%Y/%m/%d')
    end_date = end_date.strftime('%Y/%m/%d')

    # works but risky!!!! f-Strings + SQL
    QUERY = (f"""
        SELECT *
        FROM `third-being-207111.DWH.dwh_salesforce_twr`
        WHERE portfolio_id = "{portfolio_id}" """)

    query_job = client.query(QUERY)  # API request
    df = query_job.to_dataframe()

    df['dt'] = pd.to_datetime(df['dt'])
    df.set_index('dt', inplace=True)
    df.sort_index(ascending=False, inplace=True)

    twr_start = df.loc[start_date]['twr']
    twr_end = df.loc[end_date]['twr']

    return_of_period = ((twr_end + 1) / (twr_start + 1)) - 1

    return return_of_period

def get_nav(portfolio_id):

    client = bigquery.Client()
    #works but risky!!!! f-Strings + SQL
    QUERY = (f"""
        SELECT *
        FROM `third-being-207111.DWH.dwh_salesforce_nav`
        WHERE portfolio_id = "{portfolio_id}" """)

    query_job = client.query(QUERY)  # API request
    df_nav = query_job.to_dataframe()

    df_nav['dt'] = pd.to_datetime(df_nav['dt'])
    df_nav.drop(columns=['portfolio_id'], inplace=True)
    df_nav.set_index('dt', inplace=True)
    df_nav.sort_index(ascending=False, inplace=True)

    return df_nav



def get_portfolio_info(portfolio_id):

    client = bigquery.Client()
    QUERY = (f"""
        SELECT *
        FROM `third-being-207111.ANALYTICS.analytics_portfolio` """)

    query_job = client.query(QUERY)  # API request
    df = query_job.to_dataframe()
    portfolio_info = df.loc[df['portfolio_id'] == portfolio_id]

    return portfolio_info

def qplix_id_to_portfolio_id(qplix_id_dict):

    portfolio_id = {}
    portfolio_id_values = {}

    client = bigquery.Client()
    QUERY = (f"""
        SELECT *
        FROM `third-being-207111.ANALYTICS.analytics_portfolio` """)

    query_job = client.query(QUERY)  # API request
    df = query_job.to_dataframe()

    # Get the corresponding portfolio_id from qplix_portfolio_id
    for key, value in qplix_id_dict.items():

        portfolio_id[key] = df.loc[df['qplix_portfolio_id'] == value]['portfolio_id'].values

        if portfolio_id[key].size > 0:
            portfolio_id_values[key] = portfolio_id[key][0]

    return portfolio_id_values



def get_performance(qplix_id_dict):

    performance = {}
    # Get the corresponding portfolio_id from qplix_portfolio_id
    performance_dict_portfolio_id = qplix_id_to_portfolio_id(qplix_id_dict)

    for key, value in performance_dict_portfolio_id.items():
        performance[key] = get_nav(value)

    return performance

def BQ_download_performance(qplix_id_dict, start_date, end_date):

    # Get the corresponding portfolio_id from qplix_portfolio_id
    performance_dict_portfolio_id = qplix_id_to_portfolio_id(qplix_id_dict)

    performance_df = pd.DataFrame()

    for key, value in performance_dict_portfolio_id.items():
        ve = get_nav(value)
        data = get_nav(value)
        data = data.rename(columns={'nav':key})
        performance_df = pd.concat([performance_df, data], axis=1)

    return performance_df



# main function
if __name__ == '__main__':

    config = configparser.RawConfigParser()
    config.read('qplix-config.ini')
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = config['Credentials']['Goolge_credentials']

    portfolio_id = "a000Y000019ZlfSQAS"

    start_date = dt.date(2021, 5, 10)
    end_date = dt.date(2023, 5, 10)


    # return_of_period = get_twr(portfolio_id, start_date, end_date)
    # nav = get_nav(portfolio_id)
    #
    # portfolio_info = get_portfolio_info(portfolio_id)
    # Strategy = portfolio_info["portfolio_drill_1"].iloc[0]
    # Risk_class = portfolio_info["portfolio_risk_level"].iloc[0]

    qplix_id_dict = {strat: config['Client ID'][strat] for strat in config['Client ID']}
    performance_report = get_performance(qplix_id_dict)

    BQ_performance = BQ_download_performance(qplix_id_dict, start_date, end_date)

    print(BQ_performance)
