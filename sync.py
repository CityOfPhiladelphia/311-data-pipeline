import sys
import os
import csv
from datetime import date as date_obj
from datetime import datetime, timedelta
from dateutil import parser as dt_parser
import pytz
import logging
import logging.handlers
import warnings
import arrow
import click
import cx_Oracle
import pymsteams
from simple_salesforce import Salesforce
# import datum
import petl as etl
import geopetl
import requests
from requests.adapters import HTTPAdapter, Retry
import citygeo_secrets
from common import *
from config import *

#print(os.environ["ORA_TZFILE"])

# Setup Microsoft Teams connector to our webhook for channel "Citygeo Notifications"
#messageTeams = pymsteams.connectorcard(MSTEAMS_CONNECTOR)

# Setup global database vars/objects to be used between our two functions below.

# if this is set to true in the config
if TEST:
    #connect to test DB
    # DEST_DB_CONN_STRING = f'{DEST_DB_ACCOUNT}/{THREEONEONE_PASSWORD}@{DEST_TEST_DSN}'
    dest_conn = citygeo_secrets.connect_with_secrets(connect_311_test, "GIS_311","databridge-oracle/hostname-testing")
else:
    # connect to prod DB
    # DEST_DB_CONN_STRING = f'{DEST_DB_ACCOUNT}/{THREEONEONE_PASSWORD}@{DEST_PROD_DSN}'
    dest_conn = citygeo_secrets.connect_with_secrets(connect_311, "GIS_311","databridge-oracle/hostname")


# Connect to database
#print("Connecting to oracle, DNS: {}".format(DEST_DB_CONN_STRING)) 
#dest_conn = citygeo_secrets.connect_with_secrets(connect_311, "GIS_311","databridge-oracle/hostname")

dest_conn.autocommit = True
# 1200000 ms is a 20 minute timeout for really big queries
# Nvm, 0 for infinite
dest_conn.call_timeout = 0

if TEST:
    print(f'Connected to Oracle, using TEST DB and table "{DEST_TABLE}".\n')
else:
    print(f'Connected to Oracle, using PROD DB and table "{DEST_TABLE}".\n')
cur = dest_conn.cursor()


def write_log(msg):
    with open('tracker.log', 'a') as the_file:
        the_file.write(msg + '\n')

@click.command()
@click.option('--day_refresh', '-d', default=None, help='Retrieve records that were updated on a specific day, then upsert them. Ex: 2016-05-18)')
@click.option('--month_refresh', '-m', default=None, help='Retrieve records that were updated in a specific month, then upsert them. Ex: 2017-01')
@click.option('--year_refresh', '-y', default=None, help='Retrieve records that were updated in a specific year, then upsert them. Ex: 2017')
@click.option('--date_column', '-c', default='LastModifiedDate', help='Date column to select cases by from Salesforce. Default is "LastModifiedDate". You can consider using "CreatedDate" when doing full refreshes.')
def sync(day_refresh, year_refresh, month_refresh, date_column):
        salesforce_creds = citygeo_secrets.connect_with_secrets(connect_salesforce, "SalesForce API" )
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

            write_log(f'year_refresh on {year_refresh}')

            # Loop through the months
            for i in range(1,13):
                print(f'\nFetching all by last modification for month {year_refresh}-{i}')
                start_date = f'{year_refresh}-{i}-01 00:00:00 +0000'
                start_date_dt = datetime.strptime(start_date, '%Y-%m-%d %H:%M:%S %z')
                start_date_utc = convert_to_dttz(start_date_dt, utc_tz)

                write_log(f'Using start date: {start_date}')

                # less than but not equal to the next month or year so we easily capture everything
                # without nonsense about month days and leap years.
                if i == 12:
                    end_date = f'{int(year_refresh)+1}-01-01 00:00:00 +0000'
                    end_date_dt = datetime.strptime(end_date, '%Y-%m-%d %H:%M:%S %z')
                    end_date_utc = convert_to_dttz(end_date_dt, utc_tz)
                else:
                    end_date = f'{year_refresh}-{i+1}-01 00:00:00 +0000'
                    end_date_dt = datetime.strptime(end_date, '%Y-%m-%d %H:%M:%S %z')
                    end_date_utc = convert_to_dttz(end_date_dt, utc_tz)

                sf_query = SF_QUERY + f' AND ({date_column} >= {start_date_utc.isoformat()})'
                sf_query += f' AND ({date_column} < {end_date_utc.isoformat()})'
                #remove all newlines and extra whitespace in case its messing with HTML encoding
                sf_query = ' '.join(sf_query.split())
                print(sf_query)
                sf_rows = sf.query_all_iter(sf_query)

                print('Got rows.')
                process_rows(sf_rows)

        # If a month_refresh was passed in, refresh for an entire month
        elif month_refresh:
            adate = datetime.strptime(month_refresh, '%Y-%m')
            if not (int(adate.year) >= 2000) and (int(adate.year) <= 2099):
                raise Exception('Please provide a realistic year!')
            if not (int(adate.month) >= 1) and (int(adate.year) <= 12):
                raise Exception('Please provide a realistic month!')

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

            print('Got rows.')
            process_rows(sf_rows)

        # If a day was passed in, refresh for the entire day.
        elif day_refresh:
            print('Fetching records for {} only'.format(day_refresh))
            try:
                end_date = f'{day_refresh} 00:00:00 +0000'
                start_date_dt = datetime.strptime(day_refresh, '%Y-%m-%d %H:%H:%S %z')
                start_date_utc = convert_to_dttz(start_date_dt, utc_tz)
            except ValueError as e:
                #messageTeams.send()
                print('Date parameter is invalid')
                raise e
            end_date = start_date_utc + timedelta(days=1)

            sf_query += f' AND ({date_column} >= {start_date_utc.isoformat()})'
            sf_query += f' AND ({date_column} < {end_date.isoformat()})'

            sf_rows = sf.query_all_iter(sf_query)

            print('Got rows.')
            process_rows(sf_rows)


        # Otherwise, grab rows by the last updated date from the DB.
        else:
            print('Fetching new records from Salesforce by last modified date')
            max_db_query = f"select to_char(max(UPDATED_DATETIME),  'YYYY-MM-DD HH24:MI:SS.FF TZH:TZM') from {DEST_DB_ACCOUNT.upper()}.{DEST_TABLE.upper()}"
            print(f'Getting max updated date from Databridge: {max_db_query}')
            cur.execute(max_db_query)
            start_date_str = cur.fetchone()[0]
            print(f'Got {start_date_str}') 
            # Convert to UTC for salesforce querying
            start_date_dt = datetime.strptime(start_date_str, '%Y-%m-%d %H:%M:%S.%f %z')
            #start_date_utc = convert_to_dttz(start_date_dt, utc_tz)
            #start_date_utc = convert_to_dttz(start_date_dt, local_tz)


            #print(f'Converted start_date: {start_date_utc}')
            #sf_query += f' AND ({date_column} > {start_date_utc.isoformat()})'
            print(f'Converted start_date: {start_date_dt}')
            sf_query_last_where = f' AND ({date_column} > {start_date_dt.isoformat()})'
            print(f'Querying Salesforce with where: {sf_query_last_where}')
            sf_query += sf_query_last_where

            try:
                #print("Salesforce Query: ", sf_query)

                sf_rows = sf.query_all_iter(sf_query)
                #sf_debug_rows = sf.query_all_iter(sf_debug_query)
                print('Got rows.')
                process_rows(sf_rows)
            except Exception as e:
                message = "311-data-pipeline script: Couldn't query Salesforce. Error: {}".format(str(e))
                raise Exception(message)

def process_rows(sf_rows):
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
    write_log(f'Updating/adding {len(rows)} rows.')

    #Write to a temp csv to avoid memory issues:
    temp_csv = 'temp_sf_processed_rows.csv'
    #print(f'Writing to temp csv "{temp_csv}"...')
    rows = etl.fromdicts(rows)

    #print('Removing bad characters..')
    # Remove caret and single quote characters, they are bad for AGO.
    rows.convert('description', lambda a, row: a.replace(row.description, row.description.strip('<>\'')))
    rows.convert('status_notes', lambda a, row: a.replace(row.status_notes, row.status_notes.strip('<>\'')))
    # Encode in ASCII to get rid of bad special characters
    rows.convert('description', lambda u, row: u.replace(row.description, row.description.encode("ascii", "ignore".decode())))
    rows.convert('status_notes', lambda u, row: u.replace(row.status_notes, row.status_notes.encode("ascii", "ignore".decode())))
    rows.tocsv(temp_csv)

    #print('Reading from temp csv')
    rows = etl.fromcsv(temp_csv)
    etl.look(rows)

    # Truncate temp table
    print(f'Truncating temp table "{TEMP_TABLE}"...')
    cur.execute(f'truncate table {DEST_DB_ACCOUNT}.{TEMP_TABLE}')
    #autocommit on above
    #dest_conn.commit()

    #######################################
    # NOTE 6/13/2022
    # Geopetl is failing for now, so we'll do it ourselves.
    #print(f'Writing to temp table "{TEMP_TABLE}"...')
    #rows.tooraclesde(dest_conn, DEST_TEMP_TABLE)
    date_fields = ['REQUESTED_DATETIME', 'EXPECTED_DATETIME', 'UPDATED_DATETIME', 'CLOSED_DATETIME']

    prepare_stmt = f'''
     INSERT INTO {TEMP_TABLE} (service_request_id, status, service_name, service_code, description, agency_responsible, service_notice, requested_datetime, updated_datetime, expected_datetime, closed_datetime, address, zipcode, media_url, private_case, subject, type_, shape, status_notes, description_full, objectid) VALUES (:SERVICE_REQUEST_ID, :STATUS, :SERVICE_NAME, :SERVICE_CODE, :DESCRIPTION, :AGENCY_RESPONSIBLE, :SERVICE_NOTICE, TO_TIMESTAMP_TZ(:REQUESTED_DATETIME, 'YYYY-MM-DD"T"HH24:MI:SS.FFTZH:TZM'), TO_TIMESTAMP_TZ(:UPDATED_DATETIME, 'YYYY-MM-DD"T"HH24:MI:SS.FFTZH:TZM'), TO_TIMESTAMP_TZ(:EXPECTED_DATETIME, 'YYYY-MM-DD"T"HH24:MI:SS.FFTZH:TZM'), TO_TIMESTAMP_TZ(:CLOSED_DATETIME, 'YYYY-MM-DD"T"HH24:MI:SS.FFTZH:TZM'), :ADDRESS, :ZIPCODE, :MEDIA_URL, :PRIVATE_CASE, :SUBJECT, :TYPE_, SDE.ST_GEOMETRY(:SHAPE, 4326), :STATUS_NOTES, :DESCRIPTION_FULL, SDE.GDB_UTIL.NEXT_ROWID('{DEST_DB_ACCOUNT}', '{TEMP_TABLE}'))
     '''
    cur.prepare(prepare_stmt)


    with open(temp_csv) as csv_file:
        csv_reader = csv.DictReader(csv_file, delimiter=',')
        buffer_size = 1000
        val_rows = []
        next(csv_reader)
        for i, dct in enumerate(map(dict, csv_reader)):
            row_fmt = {}
            for k,v in dct.items():
                if k.upper() in date_fields:
                    v = dt_parser.parse(v) if v else None
                    v = v.isoformat() if v else None
                elif k.upper() == 'SHAPE' and not v:
                    v = 'POINT EMPTY'
                elif not v:
                    v == None
                row_fmt[f'{k.upper()}'] = v
            val_rows.append(row_fmt)
            if i> 0 and i % buffer_size == 0:
                #print(f"Reached buffer, on iteration {i}, inserting...")
                cur.executemany(None, val_rows, batcherrors=False)
                #autocommit on above
                #dest_conn.commit()
                val_rows = []

        if val_rows:
            #print("Inserting remaining rows...")
            cur.executemany(None, val_rows, batcherrors=False)
            #autocommit on above
            #dest_conn.commit() 


    ##########################


    
    #print(f'Executing UPDATE_COUNT_STMT: {UPDATE_COUNT_STMT}')
    cur.execute(UPDATE_COUNT_STMT)
    update_count = cur.fetchone()[0]
    #print(f'Deleting updated records by matching up whats in the temp table..')
    #print(f'DEL_STMT: {DEL_STMT}')
    cur.execute(DEL_STMT)
    #autocommit on above
    #dest_conn.commit()

    # Calculate number of new records added
    add_count = len(rows) - update_count

    #print(f'Appending new records to prod table {DEST_TABLE} with {dest_conn} via geopetl...')
    #rows.appendoraclesde(dest_conn, DEST_TABLE)

    #print(f'Appending new records to table {DEST_TABLE}.')
    headers_str = '''
    SERVICE_REQUEST_ID, STATUS, STATUS_NOTES, SERVICE_NAME, SERVICE_CODE, DESCRIPTION, AGENCY_RESPONSIBLE, SERVICE_NOTICE, ADDRESS, ZIPCODE, MEDIA_URL, PRIVATE_CASE, DESCRIPTION_FULL, SUBJECT, TYPE_, UPDATED_DATETIME, EXPECTED_DATETIME, CLOSED_DATETIME, REQUESTED_DATETIME, SHAPE, OBJECTID
    '''
    select_str = f'''
    SERVICE_REQUEST_ID, STATUS, STATUS_NOTES, SERVICE_NAME, SERVICE_CODE, DESCRIPTION, AGENCY_RESPONSIBLE, SERVICE_NOTICE, ADDRESS, ZIPCODE, MEDIA_URL, PRIVATE_CASE, DESCRIPTION_FULL, SUBJECT, TYPE_, UPDATED_DATETIME, EXPECTED_DATETIME, CLOSED_DATETIME, REQUESTED_DATETIME, SHAPE, SDE.GDB_UTIL.NEXT_ROWID('{DEST_DB_ACCOUNT}','{DEST_TABLE}')
    '''

    #INSERT_STMT = f'''
        #INSERT INTO {DEST_DB_ACCOUNT.upper()}.{DEST_TABLE.upper()} ({headers_str}) 
    #    SELECT {select_str} FROM {DEST_DB_ACCOUNT.upper()}.{DEST_TABLE.upper()}_TEMP WHERE SERVICE_REQUEST_ID NOT IN
    #    (SELECT SERVICE_REQUEST_ID FROM {DEST_DB_ACCOUNT.upper()}.{DEST_TABLE.upper()})
    #'''
    INSERT_STMT = f'''
    INSERT INTO {DEST_DB_ACCOUNT}.{DEST_TABLE} ({headers_str}) 
        SELECT {select_str} FROM {DEST_DB_ACCOUNT}.{TEMP_TABLE}
    '''
    #print(f'Running statement: {INSERT_STMT}')
    cur.execute(INSERT_STMT)
    #autocommit on above
    #dest_conn.commit()

    # We should have added and updated at least 1 record
    if add_count:
        print(f'Added {add_count} rows.')
    if update_count:
        print(f'Updated {update_count} rows.')
    if add_count == 0:
        warnings.warn('No records added')
    if update_count == 0:
        warnings.warn('No records updated')

    # TODO this check was causing an obscure httplib error
    # (essentially, timing out) so disabling it for now

    # Check count against Salesforce
    # sf_count = sf.query_all(SF_COUNT_QUERY)['totalSize']
    # db_count = dest_tbl.count()
    # if sf_count != db_count:
    #     warnings.warn('Salesforce has {} rows, database has {}'\
    #                             .format(sf_count, db_count))

    # If we got here, it was successful.
    #message = '311-data-pipeline script: Ran successfully. Added {}, updated {}.'.format(add_count, update_count)
    #print(message)
    # we don't need to be spammed with success messages
    #messageTeams.text(message)
    #messageTeams.send()

    write_log(f'Success.')


if __name__ == '__main__':
    sync()
    print('Done.')
