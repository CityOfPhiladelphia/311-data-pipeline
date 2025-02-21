import sys
import os
import csv
from datetime import date as date_obj
from datetime import datetime, timedelta
from dateutil import parser as dt_parser
import pytz
import psycopg2
from psycopg2.extras import execute_values
#import isoformat
import petl as etl
import geopetl
import pytz
import logging
import logging.handlers
import warnings
import click
import pymsteams
from simple_salesforce import Salesforce
import requests
from requests.adapters import HTTPAdapter, Retry
import citygeo_secrets
from common import *
from config import *
import boto3
from databridge_etl_tools.postgres.postgres import Postgres, Postgres_Connector


def connect_to_databridge(prod):
    return citygeo_secrets.connect_with_secrets(connect_databridge, 'databridge-v2/citygeo', 'databridge-v2/hostname', 'databridge-v2/hostname-testing', prod=prod)

def connect_to_salesforce():
    salesforce_creds = citygeo_secrets.connect_with_secrets(connect_salesforce, "salesforce API copy")
    sf = Salesforce(username=salesforce_creds.get('login'), \
                    password=salesforce_creds.get('password'), \
                    security_token=salesforce_creds.get('token'))
    sf.session.timeout = 540
    sf.session.adapters['https://'].max_retries = Retry(total=10, connect=5, backoff_factor=3)
    return sf

def convert_to_dttz(dt, tz):
    return dt.astimezone(tz)

def fetch_salesforce_rows(sf, query):
    return sf.query_all_iter(query)

def build_sf_query(base_query, start_date, end_date, date_column):
    query = base_query + f' AND ({date_column} >= {start_date.isoformat()})'
    query += f' AND ({date_column} < {end_date.isoformat()})'
    return ' '.join(query.split())

def get_max_updated_date(cur, table_schema, table_name):
    query = f"SELECT to_char(max(UPDATED_DATETIME), 'YYYY-MM-DD HH24:MI:SS TZH:TZM') FROM {table_schema.upper()}.{table_name.upper()}"
    cur.execute(query)
    return cur.fetchone()[0]

def process_salesforce_rows(sf_rows, field_map):
    rows = []
    for i, sf_row in enumerate(sf_rows):
        if i % 50000 == 0 and i != 0:
            print(f'DEBUG: processed {i} rows...')
            print(f"DEBUG: on CaseNumber: {sf_row['CaseNumber']}")
        rows.append(process_row(sf_row, field_map))
    return rows

def write_rows_to_csv(rows, file_path):
    etl.fromdicts(rows).tocsv(file_path)

def upload_to_s3(temp_csv, bucket, key):
    s3 = citygeo_secrets.connect_with_secrets(connect_aws_s3, 'Citygeo AWS Key Pair PROD')
    s3.upload_file(Filename=temp_csv, Bucket=bucket, Key=key)

def upsert_to_postgres(temp_csv, table_schema, table_name, prod):
    connector = citygeo_secrets.connect_with_secrets(create_dbtools_connector, 'databridge-v2/citygeo', 'databridge-v2/hostname', 'databridge-v2/hostname-testing', prod=prod)
    with Postgres(
        connector=connector,
        table_name=table_name,
        table_schema=table_schema,
        s3_bucket='citygeo-airflow-databridge2',
        s3_key='staging/citygeo/salesforce_cases_raw_pipeline_temp.csv',
        with_srid=True
    ) as postgres:
        postgres.upsert('csv')

@click.command()
@click.option('--prod', is_flag=True)
@click.option('--day_refresh', '-d', default=None, help='Retrieve records that were updated on a specific day, then upsert them. Ex: 2016-05-18)')
@click.option('--month_refresh', '-m', default=None, help='Retrieve records that were updated in a specific month, then upsert them. Ex: 2017-01')
@click.option('--year_refresh', '-y', default=None, help='Retrieve records that were updated in a specific year, then upsert them. Ex: 2017')
@click.option('--date_column', '-c', default='LastModifiedDate', help='Date column to select cases by from Salesforce. Default is "LastModifiedDate".')
def sync(prod, day_refresh, year_refresh, month_refresh, date_column):
    dest_conn = connect_to_databridge(prod)
    cur = dest_conn.cursor()

    sf = connect_to_salesforce()

    local_tz = pytz.timezone('US/Eastern')
    utc_tz = pytz.timezone('UTC')

    # Determine if we're loading data from salesforce from specific time ranges..
    if year_refresh or month_refresh or day_refresh:
        if year_refresh:
            start_date = f'{year_refresh}-01-01 00:00:00 +0000'
            start_date_utc = convert_to_dttz(datetime.strptime(start_date, '%Y-%m-%d %H:%M:%S %z'), utc_tz)

            end_date = f'{int(year_refresh)+1}-01-01 00:00:00 +0000'
            end_date_utc = convert_to_dttz(datetime.strptime(end_date, '%Y-%m-%d %H:%M:%S %z'), utc_tz)
            # Build the salesforce query
            sf_query = build_sf_query(SF_QUERY, start_date_utc, end_date_utc, date_column)

        elif month_refresh:
            adate = datetime.strptime(month_refresh, '%Y-%m')

            start_date = f'{month_refresh}-01 00:00:00 +0000'
            start_date_utc = convert_to_dttz(datetime.strptime(start_date, '%Y-%m-%d %H:%M:%S %z'), utc_tz)

            if adate.month == 12:
                end_date = f'{int(adate.year)+1}-01-01 00:00:00 +0000'
            else:
                end_date = f'{adate.year}-{int(adate.month)+1}-01 00:00:00 +0000'

            end_date_utc = convert_to_dttz(datetime.strptime(end_date, '%Y-%m-%d %H:%M:%S %z'), utc_tz)
            # Build the salesforce query
            sf_query = build_sf_query(SF_QUERY, start_date_utc, end_date_utc, date_column)

        elif day_refresh:
            start_date_utc = convert_to_dttz(datetime.strptime(f'{day_refresh} 00:00:00 +0000', '%Y-%m-%d %H:%M:%S %z'), utc_tz)
            end_date = start_date_utc + timedelta(days=1)
            # Build the salesforce query
            sf_query = build_sf_query(SF_QUERY, start_date_utc, end_date, date_column)


        # actually grab the rows from salesforce API
        sf_rows = fetch_salesforce_rows(sf, sf_query)
        # Process the rows we received from our specified date range.
        rows = process_salesforce_rows(sf_rows, FIELD_MAP)

        if not rows:
            print('Nothing received from Salesforce, nothing to update!')
            return
        else:
            # Write received rows to a CSV
            temp_csv = 'temp_sf_processed_rows.csv'
            write_rows_to_csv(rows, temp_csv)

            # Upload CSV to S3 so we can use dbtools to upsert.
            upload_to_s3(temp_csv, 'citygeo-airflow-databridge2', 'staging/citygeo/salesforce_cases_raw_pipeline_temp.csv')
            upsert_to_postgres(temp_csv, DEST_DB_ACCOUNT, DEST_TABLE, prod)

            try:
                os.remove(temp_csv)
            except Exception:
                pass

    ##########
    # Else, grab and insert rows based off our the latest modified date in our databridge tables
    else:
        # Get date from raw table, assume "enterprise" table.   
        start_date_str = get_max_updated_date(cur, 'citygeo', 'salesforce_cases_raw')
        start_date_dt = datetime.strptime(start_date_str, '%Y-%m-%d %H:%M:%S %z')
        converted_datetime = start_date_dt.astimezone(pytz.timezone('America/New_York'))
        sf_query = SF_QUERY + f' AND ({date_column} > {converted_datetime.isoformat()})'
        # Build the salesforce query
        sf_rows = fetch_salesforce_rows(sf, sf_query)
        # actually grab the rows from salesforce API
        sf_rows_processed = process_salesforce_rows(sf_rows, FIELD_MAP)

    if not sf_rows_processed:
        print('Nothing received from Salesforce, nothing to update!')
    else:
        # Write received rows to a CSV
        temp_csv = 'temp_sf_processed_rows.csv'
        write_rows_to_csv(sf_rows_processed, temp_csv)

        # Upload CSV to S3 so we can use dbtools to upsert.
        upload_to_s3(temp_csv, 'citygeo-airflow-databridge2', 'staging/citygeo/salesforce_cases_raw_pipeline_temp.csv')
        upsert_to_postgres(temp_csv, 'citygeo', 'salesforce_cases_raw', prod)

        try:
            os.remove(temp_csv)
        except Exception:
            pass

if __name__ == '__main__':
    sync()
