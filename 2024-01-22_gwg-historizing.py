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
import openpyxl
import configparser
from google.cloud import bigquery
from google.cloud import storage


class BigQueryConnector:
    def __init__(self):
        self.config = configparser.RawConfigParser()
        self.config.read('qplix-config.ini')
        self.folder_path = Path(self.config['Path']['GWG-PATH'])
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = self.config['Credentials']['Goolge_credentials']
        self.client = bigquery.Client()
        self.strategy_ls = ["Global Future", "Global", "Select", ]
        self.risklevel_ls = [str(i) for i in range(10, 110, 10)]

    def get_gwg_topup(self, report_date):
        QUERY = f"""
        -- Set the query parameter at the top
        DECLARE reference_date DATE DEFAULT "{report_date}";

        WITH LatestDepositDates AS (
          SELECT
            portfolio_id,
            vbank_number,
            MAX(CASE WHEN transaction_type = 'Deposit' THEN transaction_dt END) AS latest_deposit_date,
            MIN(CASE WHEN transaction_type = 'Deposit' THEN transaction_dt END) AS first_deposit_date
          FROM
            `third-being-207111.ANALYTICS.analytics_transaction`
          WHERE
            transaction_dt <= reference_date
          GROUP BY
            portfolio_id, vbank_number
        ),
        TransactionSummary AS (

        -- Only Clients which have a Top Up (First Deposit Date != Last Deposit Date)
        SELECT
            t.portfolio_id,
            t.vbank_number,
            SUM(CASE WHEN t.transaction_type IN ('Withdrawal', 'Deposit') THEN t.transaction_amount ELSE 0 END) AS total_amount,
            ldd.latest_deposit_date AS latest_deposit_date,
            ldd.first_deposit_date AS first_deposit_date
        FROM
            `third-being-207111.ANALYTICS.analytics_transaction` t
        JOIN
            LatestDepositDates ldd
        ON
            t.portfolio_id = ldd.portfolio_id
        WHERE
            t.transaction_dt <= reference_date 
        GROUP BY
            t.portfolio_id, t.vbank_number, ldd.latest_deposit_date, ldd.first_deposit_date
        HAVING
            ldd.latest_deposit_date != ldd.first_deposit_date
        ),
        PotentialAssets AS (
          SELECT
            a.Id AS AccountId,
            a.PotentialAsset__c,
            b.*,
            c.nav AS nav_timestamp
          FROM
            `third-being-207111.RAW.SF_ACCOUNT` a
          LEFT JOIN
            `third-being-207111.ANALYTICS.analytics_portfolio` b
          ON
            a.Id = b.account_id
          LEFT JOIN
            `third-being-207111.DWH.dwh_salesforce_nav` c
          ON
            b.portfolio_id = c.portfolio_id
          WHERE
            b.portfolio_drill_2 = "Wealth"
            AND c.dt = DATE_SUB(reference_date, INTERVAL 2 DAY)
            AND portfolio_creation_dt < reference_date
        )

        SELECT
          ts.portfolio_id,
          ts.vbank_number,
          pa.AccountId,
          pa.portfolio_creation_dt,
          ts.total_amount AS NetDeposit,
          pa.PotentialAsset__c AS PotentialAsset,
          pa.nav_timestamp AS CurrentNAV,
          ts.latest_deposit_date
        FROM
          TransactionSummary AS ts
        LEFT JOIN
          PotentialAssets AS pa
        ON
          ts.portfolio_id = pa.portfolio_id
        WHERE 
          pa.nav > 2000
          AND pa.portfolio_creation_dt >= "2022-10-03" AND portfolio_creation_dt < DATE_SUB(reference_date, INTERVAL 7 DAY) 
          AND ts.latest_deposit_date >= DATE_SUB(reference_date, INTERVAL 7 DAY) 
          AND ts.latest_deposit_date < reference_date
        """

        query_job = self.client.query(QUERY)  # API request
        df = query_job.to_dataframe()
        return df

    def routine01(self, report_date):
        report_date = dt_date(2023, 2, 13)
        # all monday dates in 2023
        monday_dates = pd.date_range(start=report_date, end=report_date, freq='W-MON').strftime('%Y-%m-%d').tolist()

        for dates in monday_dates:
            top_up_df = self.get_gwg_topup(dates)
            top_up_df['PotentialAsset'] = top_up_df['PotentialAsset'].fillna(0)
            top_up_df['NetDeposit'] = top_up_df['NetDeposit'].astype(float)
            top_up_df['PotentialAsset'] = top_up_df['PotentialAsset'].astype(float)
            top_up_df["Check"] = top_up_df.apply(lambda x: 0 if x['NetDeposit'] > x['PotentialAsset'] + 5000 else 1, axis=1)
            # top_up_df = top_up_df[top_up_df['Check'] == 0]

            # OUTPUT
            report_name = f"{dates}_GWG-TopUp-Check_weekly.xlsx"
            report_path = self.folder_path / report_name
            top_up_df.to_excel(report_path, index=False)


if __name__ == '__main__':
    BQ = BigQueryConnector()
    BQ.routine01(dt.date.today())