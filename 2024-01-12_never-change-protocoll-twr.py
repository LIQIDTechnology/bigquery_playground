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
        self.folder_path = Path(self.config['Path']['Never Change Folder'])
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = self.config['Credentials']['Goolge_credentials']
        self.client = bigquery.Client()
        self.strategy_ls = ["Global Future", "Global", "Select", ]
        self.risklevel_ls = [str(i) for i in range(10, 110, 10)]

    def check_twr_query(self, qplix_id_ls):
        """This function runs a query on the bigquery client and returns a dataframe"""

        qplix_id_ls_str = '", "'.join(qplix_id_ls)
        qplix_id_ls_str = f'"{qplix_id_ls_str}"'
        QUERY = f"""
                WITH ranked_data AS (
                  SELECT 
                    p.qplix_portfolio_id,
                    t.*,
                    ROW_NUMBER() OVER (PARTITION BY p.qplix_portfolio_id ORDER BY t.dt ASC) AS row_num
                  FROM 
                    `third-being-207111.DWH.dwh_salesforce_twr` t
                  LEFT JOIN 
                    `third-being-207111.ANALYTICS.analytics_portfolio` p
                  ON 
                    t.portfolio_id = p.portfolio_id
                  WHERE 
                    p.qplix_portfolio_id IN ({qplix_id_ls_str})
                )

                SELECT 
                  qplix_portfolio_id,
                  dt
                FROM 
                  ranked_data
                WHERE 
                  row_num = 1;
                        """
        query_job = self.client.query(QUERY)  # API request
        df = query_job.to_dataframe()

        # keep only qplix_portfolio_id which dt is <= 2022-07-01
        # convert dt to datetime
        df['dt'] = df['dt'].apply(lambda x: dt.datetime.strptime(str(x), "%Y-%m-%d").date())
        df = df[df["dt"] <= dt.date(2022, 7, 1)]

        return df["qplix_portfolio_id"].tolist()

    def calc_average_query(self, qplix_id_ls):
        qplix_id_ls_str = '", "'.join(qplix_id_ls)
        qplix_id_ls_str = f'"{qplix_id_ls_str}"'
        QUERY = f"""
        WITH RankedReturns AS (
          SELECT
            portfolio_id,
            dt,
            twr,
            LAG(twr, 1, 0) OVER (PARTITION BY portfolio_id ORDER BY dt) AS previous_twr
          FROM `third-being-207111.DWH.dwh_salesforce_twr`
        )
        , DailyReturns AS (
          SELECT
            portfolio_id,
            dt,
            twr,
            (twr + 1) / (previous_twr + 1) - 1 AS daily_twr
          FROM RankedReturns
          WHERE (twr + 1) > 0 AND (previous_twr + 1) > 0
        )
        , AdjustedReturnPre AS (
          SELECT
            portfolio_id,
            dt,
            twr,
            daily_twr,
            daily_twr + 1 AS daily_twr_plus_1,
            CASE 
              WHEN EXTRACT(DAYOFWEEK FROM dt) = 1 THEN DATE_SUB(dt, INTERVAL 2 DAY)  -- Sunday
              WHEN EXTRACT(DAYOFWEEK FROM dt) = 7 THEN DATE_SUB(dt, INTERVAL 1 DAY)  -- Saturday
              ELSE dt
            END AS adjusted_dt
          FROM DailyReturns
        )
        , AdjustedReturnPost AS (
          SELECT *, 
          EXP(SUM(LN(daily_twr + 1)) OVER (Partition BY portfolio_id, adjusted_dt)) - 1 AS adjusted_twr
          FROM AdjustedReturnPre
          ORDER BY adjusted_dt
        )
        , DailyTwrAdj AS (
          SELECT DISTINCT portfolio_id, adjusted_dt AS dt, adjusted_twr AS twr FROM AdjustedReturnPost
        )
        
        SELECT
          t.dt,
          AVG(t.twr) AS average_twr
        FROM
          `DailyTwrAdj` t
        LEFT JOIN
          `third-being-207111.ANALYTICS.analytics_portfolio` p
        ON
          t.portfolio_id = p.portfolio_id
        WHERE
          p.qplix_portfolio_id IN ({qplix_id_ls_str})
          AND t.dt >= "2022-07-01" AND t.dt <= "2023-12-31"
        GROUP BY
          t.dt
        ORDER BY
          t.dt;
        """
        query_job = self.client.query(QUERY)  # API request
        df = query_job.to_dataframe()
        return df

    def routine01(self):
        result_df = pd.DataFrame()
        for strat in self.strategy_ls:
            filename = f"eligible_clients_{strat}.csv"
            # read csv leave out first column
            eligible_df = pd.read_csv(self.folder_path / filename, index_col=0)

            for col in eligible_df.columns:
                print(col)
                qplix_id_ls = list(eligible_df[col].dropna().unique())
                eligible_plus_twr_ls = self.check_twr_query(qplix_id_ls)
                eligible_plus_twr_df = pd.DataFrame(eligible_plus_twr_ls, columns=[f"{col}"])
                result_df = pd.concat([result_df, eligible_plus_twr_df], axis=1)
                result_df.to_csv(self.folder_path / f"eligible_clients_plus_twr_ALL.csv")

    def routine02(self):
        filename = f"eligible_clients_plus_twr_ALL.csv"
        # read csv leave out first column
        eligible_plus_df = pd.read_csv(self.folder_path / filename, index_col=0)
        result_df = pd.DataFrame()
        for col in eligible_plus_df.columns:
            print(col)
            qplix_id_ls = list(eligible_plus_df[col].dropna())
            eligible_plus_twr_df = self.calc_average_query(qplix_id_ls)
            eligible_plus_twr_df['dt'] = eligible_plus_twr_df['dt'].apply(lambda x: dt.datetime.strptime(str(x), "%Y-%m-%d").date())
            # set index
            eligible_plus_twr_df.set_index('dt', inplace=True)

            # rename column to col
            eligible_plus_twr_df.rename(columns={'average_twr': f"{col}"}, inplace=True)

            result_df = pd.concat([result_df, eligible_plus_twr_df], axis=1)

        result_df.to_csv(self.folder_path / f"GROUP AVERAGES.csv")

    def routine03(self):
        count_filename = f"eligible_clients_plus_twr_ALL.csv"
        twr_filename = f"GROUP AVERAGES.csv"

        count_df = pd.read_csv(self.folder_path / count_filename, index_col=0)
        twr_df = pd.read_csv(self.folder_path / twr_filename, index_col=0)

        result_df = pd.DataFrame(columns=['dt', 'portfolio_risk_level', 'twr', 'count', 'portfolio_drill_1'])

        for col in twr_df.columns:
            rc = int(col.split(" ")[-1])
            strategy = ' '.join(col.split(" ")[:-1])
            tmp_count_df = count_df[col].dropna()
            # len of eligible clients
            count = len(tmp_count_df)
            tmp_twr_df = twr_df[col].dropna()
            # reset index
            tmp_twr_df = tmp_twr_df.reset_index()

            tmp_df = pd.DataFrame(columns=['dt', 'portfolio_risk_level', 'twr', 'count', 'portfolio_drill_1'])
            tmp_df['dt'] = tmp_twr_df['dt']
            tmp_df['portfolio_risk_level'] = rc
            tmp_df['twr'] = tmp_twr_df[col]
            tmp_df['count'] = count
            tmp_df['portfolio_drill_1'] = strategy

            result_df = pd.concat([result_df, tmp_df], axis=0)
        result_df.to_csv(self.folder_path / f"GROUP AVERAGES UPLOAD.csv", index=False)


if __name__ == '__main__':
    BQ = BigQueryConnector()
    # BQ.routine01()
    # BQ.routine02()
    BQ.routine03()