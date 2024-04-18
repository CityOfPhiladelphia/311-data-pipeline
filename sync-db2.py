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




@click.command()
@click.option('--prod', is_flag=True)
@click.option('--day_refresh', '-d', default=None, help='Retrieve records that were updated on a specific day, then upsert them. Ex: 2016-05-18)')
@click.option('--month_refresh', '-m', default=None, help='Retrieve records that were updated in a specific month, then upsert them. Ex: 2017-01')
@click.option('--year_refresh', '-y', default=None, help='Retrieve records that were updated in a specific year, then upsert them. Ex: 2017')
@click.option('--date_column', '-c', default='LastModifiedDate', help='Date column to select cases by from Salesforce. Default is "LastModifiedDate". You can consider using "CreatedDate" when doing full refreshes.')
def sync(prod, day_refresh, year_refresh, month_refresh, date_column):
    # connect to databridge
    dest_conn = citygeo_secrets.connect_with_secrets(connect_databridge, 'databridge-v2/philly311', 'databridge-v2/hostname', 'databridge-v2/hostname-testing', prod=prod)
    cur = dest_conn.cursor()

    salesforce_creds = citygeo_secrets.connect_with_secrets(connect_salesforce, "salesforce API copy")
    # Connect to Salesforce
    sf = Salesforce(username=salesforce_creds.get('login'), \
                    password=salesforce_creds.get('password'), \
                    security_token=salesforce_creds.get('token'))
    # supposedly SalesForce() takes a timeout parameter, but it doesn't appear to work.
    # Instead, we can apparently set the tmeout anyway by inserting our own request session
    #session = requests.Session()
    #session.timeout = 540
    #sf.session = session

    # Set a custom timeout in the requests session object directly
    sf.session.timeout = 540
    # Set a custom amount to retry
    # https://github.com/simple-salesforce/simple-salesforce/issues/402#issuecomment-1109085548
    sf.session.adapters['https://'].max_retries = Retry(total=10, connect=5, backoff_factor=3)

    # Used to formulate Salesforce query below
    sf_query = SF_QUERY


    local_tz = pytz.timezone('US/Eastern')
    utc_tz = pytz.timezone('UTC')

    def convert_to_dttz(dt, tz):
        dt_tz = dt.astimezone(tz)
        return dt_tz

    # If a year was passed in, refresh for an entire year a month at a time
    if year_refresh:
        if not (int(year_refresh) >= 2000) and (int(year_refresh) <= 2099):
            raise Exception('Please provide a realistic year!')

        print(f'year_refresh on {year_refresh}')

        print(f'\nFetching all by last modification for year: {year_refresh}')
        start_date = f'{year_refresh}-01-01 00:00:00 +0000'
        start_date_dt = datetime.strptime(start_date, '%Y-%m-%d %H:%M:%S %z')
        start_date_utc = convert_to_dttz(start_date_dt, utc_tz)

        end_date = f'{int(year_refresh)+1}-01-01 00:00:00 +0000'
        end_date_dt = datetime.strptime(end_date, '%Y-%m-%d %H:%M:%S %z')
        end_date_utc = convert_to_dttz(end_date_dt, utc_tz)

        print(f'Using start date: {start_date}')
        print(f'End date: {end_date}')

        sf_query = SF_QUERY + f' AND ({date_column} >= {start_date_utc.isoformat()})'
        sf_query += f' AND ({date_column} < {end_date_utc.isoformat()})'
        #remove all newlines and extra whitespace in case its messing with HTML encoding
        sf_query = ' '.join(sf_query.split())
        print(sf_query)
        sf_rows = sf.query_all_iter(sf_query)

        # Note: cannot find a way to get length of sf_rows without running through it
        # as it is a generator, not a list or dict.
        print('Got rows')

    # If a month_refresh was passed in, refresh for an entire month
    elif month_refresh:
        adate = datetime.strptime(month_refresh, '%Y-%m')
        if not (int(adate.year) >= 2000) and (int(adate.year) <= 2099):
            raise Exception('Please provide a realistic year!')
        if not (int(adate.month) >= 1) and (int(adate.year) <= 12):
            raise Exception('Please provide a real month number!')

        print(f'\nFetching all by last modification for month {month_refresh}')
        start_date = f'{month_refresh}-01 00:00:00 +0000'
        start_date_dt = datetime.strptime(start_date, '%Y-%m-%d %H:%M:%S %z')
        start_date_utc = convert_to_dttz(start_date_dt, utc_tz)

        # less than but not equal to the next month or year so we easily capture everything
        # without nonsense about month days and leap years.
        if adate.month == 12:
            end_date = f'{int(adate.year)+1}-01-01 00:00:00 +0000'
            end_date_dt = datetime.strptime(end_date, '%Y-%m-%d %H:%M:%S %z')
            end_date_utc = convert_to_dttz(end_date_dt, utc_tz)
        else:
            end_date = f'{adate.year}-{int(adate.month)+1}-01 00:00:00 +0000'
            end_date_dt = datetime.strptime(end_date, '%Y-%m-%d %H:%M:%S %z')
            end_date_utc = convert_to_dttz(end_date_dt, utc_tz)

        sf_query = SF_QUERY + f' AND ({date_column} >= {start_date_utc.isoformat()})'
        sf_query += f' AND ({date_column} < {end_date_utc.isoformat()})'
        #remove all newlines and extra whitespace in case its messing with HTML encoding
        sf_query = ' '.join(sf_query.split())
        print(sf_query)
        sf_rows = sf.query_all_iter(sf_query)

        # Note: cannot find a way to get length of sf_rows without running through it
        # as it is a generator, not a list or dict.
        print(f'Got rows.')

    # If a day was passed in, refresh for the entire day.
    elif day_refresh:
        print('Fetching records for {} only'.format(day_refresh))
        try:
            end_date = f'{day_refresh} 00:00:00 +0000'

            start_date_dt = datetime.strptime(end_date, '%Y-%m-%d %H:%M:%S %z')
            #start_date_dt = datetime.strptime(day_refresh, 'YYYY-MM-DD HH24:MI:SS+TZH')
            #start_date_dt = datetime.strptime(day_refresh, 'YYYY-MM-DD')
            start_date_utc = convert_to_dttz(start_date_dt, utc_tz)
        except ValueError as e:
            #messageTeams.send()
            print('Date parameter is invalid')
            raise e
        end_date = start_date_utc + timedelta(days=1)

        sf_query += f' AND ({date_column} >= {start_date_utc.isoformat()})'
        sf_query += f' AND ({date_column} < {end_date.isoformat()})'

        sf_rows = sf.query_all_iter(sf_query)

        # Note: cannot find a way to get length of sf_rows without running through it
        # as it is a generator, not a list or dict.
        print('Got rows.')

    # Otherwise, grab rows by the last updated date from the DB.
    else:
        print('Fetching new records from Salesforce by last modified date')
        max_db_query = f"select to_char(max(UPDATED_DATETIME),  'YYYY-MM-DD HH24:MI:SS TZH:TZM') from {DEST_DB_ACCOUNT.upper()}.{DEST_TABLE.upper()}"
        print(f'Getting max updated date from Databridge: {max_db_query}')
        cur.execute(max_db_query)
        start_date_str = cur.fetchone()[0]
        print(f'Got {start_date_str}')
        # Make sure it's in our timezone
        est = pytz.timezone('America/New_York')
        start_date_dt = datetime.strptime(start_date_str, '%Y-%m-%d %H:%M:%S %z')
        converted_datetime = start_date_dt.astimezone(est)

        print(f'Converted start_date: {converted_datetime}')
        sf_query_last_where = f' AND ({date_column} > {converted_datetime.isoformat()})'
        print(f'Querying Salesforce with where: {sf_query_last_where}')
        sf_query += sf_query_last_where

        try:
            #print("Salesforce Query: ", sf_query)
            sf_rows = sf.query_all_iter(sf_query)
            #sf_debug_rows = sf.query_all_iter(sf_debug_query)
            # Note: cannot find a way to get length of sf_rows without running through it
            # as it is a generator, not a list or dict.
            print('Got rows.')
        except Exception as e:
            message = "Error: {}".format(str(e))
            raise Exception(message)

    # --------------------------- #
    print('Processing rows...')
    rows = []

    for i, sf_row in enumerate(sf_rows):
        if i % 50000 == 0 and i != 0:
            print(f'DEBUG: processed {i} rows...')
            #print(sf_row)
            print(f"DEBUG: on CaseNumber: {sf_row['CaseNumber']}")
        # process_row() is from common.py
        rows.append(process_row(sf_row, FIELD_MAP))

    if not rows:
        print('Nothing received from Salesforce, nothing to update!')
        return

    print(f'Updating/adding {len(rows)} rows.')
    print(f'Updating/adding {len(rows)} rows.')

    #Write to a temp csv to avoid memory issues:
    temp_csv = 'temp_sf_processed_rows.csv'
    print(f'Writing to temp csv "{temp_csv}"...')
    rows = etl.fromdicts(rows)

    rows.tocsv(temp_csv)

    #print('Reading from temp csv')
    rows = etl.fromcsv(temp_csv)
    etl.look(rows)

    if not rows:
        print('No rows found!')
        sys.exit(1)
    else:
        # upload to S3 so dbtools can use it
        s3 = citygeo_secrets.connect_with_secrets(connect_aws_s3, 'Citygeo AWS Key Pair PROD')
        s3.upload_file(Filename=temp_csv,
                    Bucket='citygeo-airflow-databridge2',
                    Key='staging/philly311/salesforce_cases_raw_pipeline_temp.csv')

        # Load via databridge-et-tools
        connector = citygeo_secrets.connect_with_secrets(create_dbtools_connector, 'databridge-v2/philly311', 'databridge-v2/hostname', 'databridge-v2/hostname-testing', prod=prod)
        with Postgres(
            connector=connector,
            table_name='salesforce_cases_raw',
            table_schema='philly311',
            s3_bucket='citygeo-airflow-databridge2',
            s3_key='staging/philly311/salesforce_cases_raw_pipeline_temp.csv', 
            with_srid=True) as postgres: 
            postgres.upsert('csv')
        
        print(f'Success.')

    try:
        print(f'Attempting to remove {temp_csv}')
        os.remove(temp_csv)
    except:
        pass


if __name__ == '__main__':
    sync()
    print('Done.')
