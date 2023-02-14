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
from common import *
from config import *

#print(os.environ["ORA_TZFILE"])

# Setup Microsoft Teams connector to our webhook for channel "Citygeo Notifications"
messageTeams = pymsteams.connectorcard(MSTEAMS_CONNECTOR)


# Setup global database vars/objects to be used between our two functions below.

print('PASS:    ', THREEONEONE_PASSWORD)
print('DB:    ', DATABRIDGE_DB)
DEST_DB_CONN_STRING = f'gis_311/{THREEONEONE_PASSWORD}@(DESCRIPTION=(ADDRESS_LIST=(ADDRESS=(PROTOCOL=TCP)(HOST={DATABRIDGE_HOST})(PORT=1521)))(CONNECT_DATA=(SERVICE_NAME={DATABRIDGE_DB})))'
# Connect to database
print("Connecting to oracle, DNS: {}".format(DEST_DB_CONN_STRING)) 
dest_conn = cx_Oracle.connect(DEST_DB_CONN_STRING)
cur = dest_conn.cursor()


@click.command()
@click.option('--day', '-d', help='Retrieve records that were updated on a specific day (e.g. 2016-05-18). This is mostly for debugging and maintenance purposes.')
@click.option('--full_refresh', is_flag=True, help='Full refresh by chunking through service_request_ids')
def sync(day, full_refresh):
        # Connect to Salesforce
        sf = Salesforce(username=SF_USER, \
                        password=SF_PASSWORD, \
                        security_token=SF_TOKEN)

        # Used to formulate Salesforce query below
        sf_query = SF_QUERY

        # supposedly SalesForce() takes a timeout parameter, but I get an unexpected keyword when I try
        # this hack apparently sets the timeout anyway by inserting our own request session
        session = requests.Session()
        session.timeout = 540
        sf.session = session

        local_tz = pytz.timezone('US/Eastern')
        utc_tz = pytz.timezone('UTC')

        def convert_to_dttz(dt, tz):
            dt_tz = dt.astimezone(tz)
            return dt_tz


        if full_refresh:
            # Completely sync up our dataset with Salesforce's data
            # Get max CaseNumber (service_request_id) so we can chunk up to that.
            max_casenumber_query = "SELECT max(CaseNumber) FROM Case"
            #max_casenumber_query = "SELECT max(CaseNumber) FROM Case WHERE ZipCode__c != 'NULL' AND " + SF_WHERE
            print(f'Getting max CaseNumber with: {max_casenumber_query}')
            max_casenumber = sf.query_all(max_casenumber_query)
            max_casenumber = int(max_casenumber['records'][0]['expr0'])
            print(max_casenumber)

            # Getting the last row just for debug purposes
            #last_row_query = sf_query + f"AND CaseNumber = '{max_casenumber}'"
            #print(f'Getting latest row with: {last_row_query}')
            #last_row = sf.query_all(last_row_query)
            #print(last_row)


            # Chunk by 100,000 rows
            times_to_loop = int(max_casenumber / 100000)
            remainder = int(max_casenumber % 100000)
            print(f'times_to_loop: {times_to_loop}')
            print(f'remainder: {remainder}')

            for i in range(1,times_to_loop+1):
                top = i * 100000 
                bottom = top - 100000
                # Because salesforce CaseNumbers are strings and some start with zeros, we need to cast
                # them to make this work. Otherwise we get a billion rows regardless.
                sf_query = SF_QUERY + f" AND CaseNumber > '{bottom}'"
                sf_query +=  f" AND CaseNumber <= '{top}'"
                #remove all newlines and extra whitespace in case its messing with HTML encoding
                sf_query = ' '.join(sf_query.split())
                print(f'\nFetching records with CaseNumber (service_request_id) > {bottom} and <= {top}')
                print(sf_query)

                sf_rows = sf.query_all_iter(sf_query)

                print('Got rows.')
                process_rows(sf_rows)


            # Fetch remainder if not 0 (somehow we're divisible by 100,000)
            if remainder:
                top = (times_to_loop * 100000) + remainder
                bottom = (times_to_loop * 100000)

                # Because salesforce CaseNumbers are strings and some start with zeros, we need to cast
                # them to make this work. Otherwise we get a billion rows regardless.
                sf_query = SF_QUERY + f" AND CaseNumber > '{bottom}'"
                sf_query +=  f" AND CaseNumber <= '{top}'"
                #remove all newlines and extra whitespace in case its messing with HTML encoding
                sf_query = ' '.join(sf_query.split())
                print(f'Fetching remainder with CaseNumber (service_request_id) > {bottom} and <= {top}')
                print(sf_query)

                sf_rows = sf.query_all_iter(sf_query)

                print('Got rows.')
                process_rows(sf_rows)


        # If a start date was passed in, handle it.
        elif day:
            print('Fetching records for {} only'.format(day))
            try:
                start_date_dt = datetime.strptime(day, '%Y-%m-%d')
                start_date_utc = convert_to_dttz(start_date_dt, utc_tz)
            except ValueError as e:
                message = '311-data-pipeline script: Value Error! {}'.format(str(e))
                messageTeams.text(message)
                messageTeams.send()
                raise AssertionError('Date parameter is invalid')
            end_date = start_date_utc + timedelta(days=1)

            sf_query += ' AND (LastModifiedDate >= {})'.format(start_date_utc.isoformat())
            sf_query += ' AND (LastModifiedDate < {})'.format(end_date.isoformat())

            sf_rows = sf.query_all_iter(sf_query)

            print('Got rows.')
            process_rows(sf_rows)


        # Otherwise, grab the last updated date from the DB.
        else:
            print('Getting last updated date...')
            max_db_query = f"select to_char(max(UPDATED_DATETIME),  'YYYY-MM-DD HH24:MI:SS.FF TZH:TZM') from {DEST_DB_ACCOUNT.upper()}.{DEST_TABLE.upper()}"
            print(f'Getting max updated date from Databridge: {max_db_query}')
            cur.execute(max_db_query)
            start_date_str = cur.fetchone()[0]
            print(f'Got {start_date_str}') 
            # Convert to UTC for salesforce querying
            start_date_dt = datetime.strptime(start_date_str, '%Y-%m-%d %H:%M:%S.%f %z')
            start_date_utc = convert_to_dttz(start_date_dt, utc_tz)


            print(f'Converted start_date: {start_date_utc}')
            sf_query += ' AND (LastModifiedDate > {})'.format(start_date_utc.isoformat())

        try:
            print('Fetching new records from Salesforce...')
            print("Salesforce Query: ", sf_query)

            sf_rows = sf.query_all_iter(sf_query)
            #sf_debug_rows = sf.query_all_iter(sf_debug_query)
            print('Got rows.')
            process_rows(sf_rows)
        except Exception as e:
            message = "311-data-pipeline script: Couldn't query Salesforce. Error: {}".format(str(e))
            messageTeams.text(message)
            messageTeams.send()
            raise Exception(message)

def process_rows(sf_rows):



    print('Processing rows...')
    rows = []

    for i, sf_row in enumerate(sf_rows):
        if i % 20000 == 0 and i != 0:
            print(f'processed {i} rows...')
            #print(sf_row)
            print(f"on CaseNumber: {sf_row['CaseNumber']}")
        # process_row() is from common.py
        rows.append(process_row(sf_row, FIELD_MAP))

    if not rows:
        print('Nothing received from Salesforce, nothing to update!')
        return

    print(f'Uploading {len(rows)} rows.')

    #Write to a temp csv to avoid memory issues:
    temp_csv = 'temp_sf_processed_rows.csv'
    print(f'Writing to temp csv "{temp_csv}"...')
    rows = etl.fromdicts(rows)

    print('Removing bad characters..')
    # Remove caret and single quote characters, they are bad for AGO.
    rows.convert('description', lambda a, row: a.replace(row.description, row.description.strip('<>\'')))
    rows.convert('status_notes', lambda a, row: a.replace(row.status_notes, row.status_notes.strip('<>\'')))
    # Encode in ASCII to get rid of bad special characters
    rows.convert('description', lambda u, row: u.replace(row.description, row.description.encode("ascii", "ignore".decode())))
    rows.convert('status_notes', lambda u, row: u.replace(row.status_notes, row.status_notes.encode("ascii", "ignore".decode())))
    rows.tocsv(temp_csv)

    print('Reading from temp csv')
    rows = etl.fromcsv(temp_csv)
    etl.look(rows)

    TEMP_TABLE = DEST_DB_ACCOUNT.upper() + '.' + DEST_TEMP_TABLE 
    # Truncate temp table
    print(f'Truncating temp table "{DEST_TEMP_TABLE}"...')
    cur.execute(f'truncate table {DEST_TEMP_TABLE}')
    dest_conn.commit()

    #######################################
    # NOTE 6/13/2022
    # Geopetl is failing for now, so we'll do it ourselves.
    print(f'Writing to temp table "{TEMP_TABLE}"...')
    #rows.tooraclesde(dest_conn, DEST_TEMP_TABLE)
    date_fields = ['REQUESTED_DATETIME', 'EXPECTED_DATETIME', 'UPDATED_DATETIME', 'CLOSED_DATETIME']

    prepare_stmt = f'''
     INSERT INTO {TEMP_TABLE} (service_request_id, status, service_name, service_code, description, agency_responsible, service_notice, requested_datetime, updated_datetime, expected_datetime, closed_datetime, address, zipcode, media_url, private_case, subject, type_, shape, status_notes, description_full, objectid) VALUES (:SERVICE_REQUEST_ID, :STATUS, :SERVICE_NAME, :SERVICE_CODE, :DESCRIPTION, :AGENCY_RESPONSIBLE, :SERVICE_NOTICE, TO_TIMESTAMP_TZ(:REQUESTED_DATETIME, 'YYYY-MM-DD"T"HH24:MI:SS.FFTZH:TZM'), TO_TIMESTAMP_TZ(:UPDATED_DATETIME, 'YYYY-MM-DD"T"HH24:MI:SS.FFTZH:TZM'), TO_TIMESTAMP_TZ(:EXPECTED_DATETIME, 'YYYY-MM-DD"T"HH24:MI:SS.FFTZH:TZM'), TO_TIMESTAMP_TZ(:CLOSED_DATETIME, 'YYYY-MM-DD"T"HH24:MI:SS.FFTZH:TZM'), :ADDRESS, :ZIPCODE, :MEDIA_URL, :PRIVATE_CASE, :SUBJECT, :TYPE_, SDE.ST_GEOMETRY(:SHAPE, 4326), :STATUS_NOTES, :DESCRIPTION_FULL, SDE.GDB_UTIL.NEXT_ROWID('{TEMP_TABLE.split('.')[0]}', '{TEMP_TABLE.split('.')[1]}'))
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
                dest_conn.commit()
                val_rows = []

        if val_rows:
            print("Inserting remaining rows...")
            cur.executemany(None, val_rows, batcherrors=False)
            dest_conn.commit() 


    ##########################


    print(f'Executing UPDATE_COUNT_STMT: {UPDATE_COUNT_STMT}')
    cur.execute(UPDATE_COUNT_STMT)
    update_count = cur.fetchone()[0]
    print(f'Deleting updated records by matching up whats in the temp table..')
    print(f'DEL_STMT: {DEL_STMT}')
    cur.execute(DEL_STMT)
    dest_conn.commit()

    # Calculate number of new records added
    add_count = len(rows) - update_count

    #print(f'Appending new records to prod table {DEST_TABLE} with {dest_conn} via geopetl...')
    #rows.appendoraclesde(dest_conn, DEST_TABLE)

    print(f'Appending new records to prod table {DEST_TABLE}.')
    headers_str = '''
    SERVICE_REQUEST_ID, STATUS, STATUS_NOTES, SERVICE_NAME, SERVICE_CODE, DESCRIPTION, AGENCY_RESPONSIBLE, SERVICE_NOTICE, ADDRESS, ZIPCODE, MEDIA_URL, PRIVATE_CASE, DESCRIPTION_FULL, SUBJECT, TYPE_, UPDATED_DATETIME, EXPECTED_DATETIME, CLOSED_DATETIME, REQUESTED_DATETIME, SHAPE, OBJECTID
    '''
    select_str = f'''
    SERVICE_REQUEST_ID, STATUS, STATUS_NOTES, SERVICE_NAME, SERVICE_CODE, DESCRIPTION, AGENCY_RESPONSIBLE, SERVICE_NOTICE, ADDRESS, ZIPCODE, MEDIA_URL, PRIVATE_CASE, DESCRIPTION_FULL, SUBJECT, TYPE_, UPDATED_DATETIME, EXPECTED_DATETIME, CLOSED_DATETIME, REQUESTED_DATETIME, SHAPE, SDE.GDB_UTIL.NEXT_ROWID('{DEST_DB_ACCOUNT.upper()}','{DEST_TABLE.upper()}')
    '''

    #INSERT_STMT = f'''
        #INSERT INTO {DEST_DB_ACCOUNT.upper()}.{DEST_TABLE.upper()} ({headers_str}) 
    #    SELECT {select_str} FROM {DEST_DB_ACCOUNT.upper()}.{DEST_TABLE.upper()}_TEMP WHERE SERVICE_REQUEST_ID NOT IN
    #    (SELECT SERVICE_REQUEST_ID FROM {DEST_DB_ACCOUNT.upper()}.{DEST_TABLE.upper()})
    #'''
    INSERT_STMT = f'''
    INSERT INTO {DEST_DB_ACCOUNT.upper()}.{DEST_TABLE.upper()} ({headers_str}) 
        SELECT {select_str} FROM {DEST_DB_ACCOUNT.upper()}.{DEST_TABLE.upper()}_TEMP
    '''
    print(f'Running statement: {INSERT_STMT}')
    cur.execute(INSERT_STMT)
    dest_conn.commit()

    # We should have added and updated at least 1 record
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



if __name__ == '__main__':
    sync()
