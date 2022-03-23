import sys
import os
from datetime import date as date_obj
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
from common import *
from config import *

# Setup Microsoft Teams connector to our webhook for channel "Citygeo Notifications"
messageTeams = pymsteams.connectorcard(MSTEAMS_CONNECTOR)

# LOGGING
logger = logging.getLogger(__name__)
logger.setLevel(getattr(logging, LOG_LEVEL))
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

# File handler
log_file_parent_dir = os.path.dirname(os.path.abspath(LOG_PATH))
os.makedirs(log_file_parent_dir, exist_ok=True)
logging_file_handler = logging.handlers.RotatingFileHandler(\
                LOG_PATH,\
                maxBytes=10*1024*1024,\
                backupCount=5\
)
logging_file_handler.setFormatter(formatter)
logger.addHandler(logging_file_handler)


@click.command()
@click.option('--date', '-d', help='Retrieve records that were updated on a specific date (e.g. 2016-05-18). This is mostly for debugging and maintenance purposes.')
@click.option('--alerts/--no-alerts', default=True, help='Turn alerts on/off')
@click.option('--verbose', '-v', is_flag=True, help='Print logging statements to the console')
def sync(date, alerts, verbose):
    status = 'ERROR'

    with warnings.catch_warnings(record=True) as w:
        try:
            if verbose:
                console_handler = logging.StreamHandler()
                console_handler.setLevel(logging.DEBUG)
                console_handler.setFormatter(formatter)
                logger.addHandler(console_handler)

            logger.info('Starting...')
            start = arrow.now()

            # Connect to Salesforce
            sf = Salesforce(username=SF_USER, \
                            password=SF_PASSWORD, \
                            security_token=SF_TOKEN)

            # Connect to database
            logger.info("Connecting to oracle, DNS: {}".format(DEST_DB_CONN_STRING)) 
            dest_conn = cx_Oracle.connect(DEST_DB_CONN_STRING)

            # Truncate temp table
            logger.info('Truncating temp table...')
            dest_cur = dest_conn.cursor()
            dest_cur.execute(f'truncate table {DEST_TEMP_TABLE}')
            dest_conn.commit()

            # Formulate Salesforce query
            sf_query = SF_QUERY
            #sf_debug_query = SF_DEBUG_QUERY

            # If a start date was passed in, handle it.
            if date:
                warnings.warn('Fetched records for {} only'.format(date))
                try:
                    date_comps = [int(x) for x in date.split('-')]
                    start_date = arrow.get(date_obj(*date_comps), 'US/Eastern')\
                                      .to('Etc/UTC')
                except ValueError as e:
                    message = '311-data-pipeline script: Value Error! {}'.format(str(e))
                    messageTeams.text(message)
                    messageTeams.send()
                    raise HandledError('Date parameter is invalid')
                end_date = start_date.shift(days=+1)

                sf_query += ' AND (LastModifiedDate >= {})'.format(start_date)
                sf_query += ' AND (LastModifiedDate < {})'.format(end_date)

            # Otherwise, grab the last updated date from the DB.
            else:
                logger.info('Getting last updated date...')
                dest_cur.execute('select max({}) from {}'\
                                            .format(DEST_UPDATED_FIELD, DEST_TABLE))
                start_date_str = dest_cur.fetchone()[0]
                start_date = arrow.get(start_date_str, 'US/Eastern').to('Etc/UTC')
                sf_query += ' AND (LastModifiedDate > {})'.format(start_date.isoformat())
                #sf_debug_query += ' AND (LastModifiedDate > {})'.format(start_date.isoformat())

            logger.info('Fetching new records from Salesforce...')
            #print("Salesforce Query: ", sf_query)
            try:
                sf_rows = sf.query_all_iter(sf_query)
                #sf_debug_rows = sf.query_all_iter(sf_debug_query)
            except Exception as e:
                message = "311-data-pipeline script: Couldn't query Salesforce. Error: {}".format(str(e))
                messageTeams.text(message)
                messageTeams.send()
                raise HandledError(message)
            logger.info('Processing rows...')
            rows = []
            for i, sf_row in enumerate(sf_rows):
                #if i % 1000 == 0:
                #    print(f'processed {i} rows...')
                rows.append(process_row(sf_row, FIELD_MAP))

            #Write to a temp csv to avoid memory issues:
            temp_csv = 'temp_sf_processed_rows.csv'
            logger.info(f'Writing to temp csv {temp_csv}...')
            rows = etl.fromdicts(rows)
            rows.tocsv(temp_csv)

            logger.info('Reading from temp csv and writing to temp table...')
            rows = etl.fromcsv(temp_csv)
            try:
                rows.tooraclesde(dest_conn, DEST_TEMP_TABLE)
            except Exception as e:
                raise e
            logger.info('Deleting updated records...')
            dest_cur.execute(UPDATE_COUNT_STMT)
            update_count = dest_cur.fetchone()[0]
            try:
                dest_cur.execute(DEL_STMT)
                dest_conn.commit()
            except Exception as e:
                raise e

            # Calculate number of new records added
            add_count = len(rows) - update_count

            logger.info('Appending new records to prod table...')
            try:
                rows.appendoraclesde(dest_conn, DEST_TABLE)
            except Exception as e:
                raise e

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
            status = 'SUCCESS'
            message = '311-data-pipeline script: Ran successfully. Added {}, updated {}.'.format(add_count, update_count)
            logger.info(message)
            # we don't need to be spammed with success messages
            #messageTeams.text(message)
            #messageTeams.send()

        except Exception as e:
            message = '311-data-pipeline script: Error! Unhandled error: {}'.format(str(e))
            logger.exception(message)
            messageTeams.text(message)
            messageTeams.send()

        finally:
            if alerts:
                msg = '[311] {} - {}'.format(__file__, status)
                if status == 'SUCCESS':
                    msg += ' - {} added, {} updated'\
                                    .format(add_count, update_count)
                if len(w) > 0:
                    msg += ' - {}.'.format('; '.join([str(x.message) for x in w]))


if __name__ == '__main__':
    sync()
