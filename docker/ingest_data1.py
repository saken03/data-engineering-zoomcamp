#!/usr/bin/env python
# coding: utf-8

import os
import argparse

from time import time

import pandas as pd
from sqlalchemy import create_engine


def main(params):
    user = params.user
    password = params.password
    host = params.host 
    port = params.port 
    db = params.db
    table_name = params.table_name
    url = params.url
    
    # the backup files are gzipped, and it's important to keep the correct extension
    # for pandas to be able to open the file
    if url.endswith('.csv.gz'):
        csv_name = 'output.csv.gz'
    else:
        csv_name = 'output.csv'

    os.system(f"wget {url} -O {csv_name}")

    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

    df_iter = pd.read_csv(csv_name, iterator=True, chunksize=100000, low_memory=False)

    df = next(df_iter)

    # df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)
    # df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)

    df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')

    df.to_sql(name=table_name, con=engine, if_exists='append')


    while True: 

        try:
            t_start = time()
            
            df = next(df_iter)

            # df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)
            # df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)

            df.to_sql(name=table_name, con=engine, if_exists='append')

            t_end = time()

            print('inserted another chunk, took %.3f second' % (t_end - t_start))

        except StopIteration:
            print("Finished ingesting data into the postgres database")
            break

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Ingest CSV data to Postgres')

    parser.add_argument('--user', required=True, help='user name for postgres')
    parser.add_argument('--password', required=True, help='password for postgres')
    parser.add_argument('--host', required=True, help='host for postgres')
    parser.add_argument('--port', required=True, help='port for postgres')
    parser.add_argument('--db', required=True, help='database name for postgres')
    parser.add_argument('--table_name', required=True, help='name of the table where we will write the results to')
    parser.add_argument('--url', required=True, help='url of the csv file')

    args = parser.parse_args()

    main(args)

# SELECT COUNT(*) AS total_trips
# FROM green_taxi_data
# WHERE DATE(lpep_pickup_datetime) = '2019-09-18'
#   AND DATE(lpep_dropoff_datetime) = '2019-09-18';

# SELECT DATE(lpep_pickup_datetime) AS pickup_day, MAX(trip_distance) AS longest_distance
# FROM green_taxi_data
# GROUP BY pickup_day
# ORDER BY longest_distance DESC
# LIMIT 1;

# SELECT tz."Borough", SUM(gt.total_amount) AS total_amount_sum
# FROM green_taxi_data gt
# JOIN taxi_zone tz ON gt."PULocationID" = tz."LocationID"
# WHERE DATE(gt.lpep_pickup_datetime) = '2019-09-18'
# 	AND tz."Borough" IS NOT NULL
# GROUP BY tz."Borough"
# HAVING SUM(gt.total_amount) > 50000
# ORDER BY total_amount_sum DESC
# LIMIT 3;

# select tz."Zone" as dropoff_zone, max(gt.tip_amount) as largest_tip
# from green_taxi_data gt
# join taxi_zone t on gt."PULocationID" = t."LocationID"
# join taxi_zone tz on gt."DOLocationID" = tz."LocationID"
# where t."Zone" = 'Astoria'
# and DATE(gt.lpep_pickup_datetime) between '2019-09-01' and '2019-09-30'
# group by 1
# order by 2 desc