import sys
import os
import logging
import logging.handlers
import arrow
import click
import cx_Oracle
from simple_salesforce import Salesforce
from simple_salesforce.api import SalesforceMalformedRequest
import datum
from slacker import Slacker
from common import *
from config import *

# Set up logging
logger = logging.getLogger(__name__)
logger.setLevel(getattr(logging, LOG_LEVEL))
log_path = os.path.abspath(LOG_PATH)
log_par_dir = os.path.dirname(log_path)
os.makedirs(log_par_dir, exist_ok=True)
handler = logging.handlers.RotatingFileHandler(\
                LOG_PATH,\
                maxBytes=10*1024*1024,\
                backupCount=5\
)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

class HandledError(Exception):
    pass

@click.command()
@click.option('--date', '-d', help='Retrieve records that were updated on a specific date (e.g. 2016-05-18). This is mostly for debugging and maintenance purposes.')
@click.option('--alerts/--no-alerts', default=True, help='Turn alerts on/off')
@click.option('--verbose', '-v', is_flag=True, help='Pring logging statements to the console')
def sync(date, alerts, verbose):
    status = 'ERROR'
    notes = []
    
    try:
        if verbose:
            console_handler = logging.StreamHandler()
            console_handler.setLevel(logging.DEBUG)
            console_handler.setFormatter(formatter)
            logger.addHandler(console_handler)

        logger.info('Starting...')
        start = arrow.now()
        
        # Connect to Slack
        slack = Slacker(SLACK_TOKEN)

        # Connect to Salesforce
        sf = Salesforce(username=SF_USER, \
                        password=SF_PASSWORD, \
                        security_token=SF_TOKEN)

        # Connect to database
        dest_db = datum.connect(DEST_DB_DSN)
        dest_tbl = dest_db[DEST_TABLE]
        tmp_tbl = dest_db[DEST_TEMP_TABLE]

        logger.info('Truncating temp table...')
        tmp_tbl.delete()

        sf_query = SF_QUERY

        # If a start date was passed in, handle it.
        if date:
            notes.append('Fetched records for {} only'.format(date))

            try:
                start_date = arrow.get(date)
            except ValueError:
                raise HandledError('Date parameter is invalid')
            end_date = start_date.replace(days=1)

            sf_query += ' AND (LastModifiedDate >= {})'.format(start_date)
            sf_query += ' AND (LastModifiedDate < {})'.format(end_date)

        # Otherwise, grab the last updated date from the DB.
        else:
            logger.info('Getting last updated date...')
            start_date = dest_db.execute('select max({}) from {}'\
                                        .format(DEST_UPDATED_FIELD, DEST_TABLE))[0]
            start_date = arrow.get(start_date)
            sf_query += ' AND (LastModifiedDate > {})'.format(start_date.isoformat())

        logger.info('Fetching new records from Salesforce...')
        try:
            sf_rows = sf.query_all(sf_query)['records']
        except SalesforceMalformedRequest:
            raise HandledError('Could not query Salesforce')

        logger.info('Processing rows...')
        rows = [process_row(sf_row, FIELD_MAP) for sf_row in sf_rows]

        logger.info('Writing to temp table...')
        tmp_tbl.write(rows)

        logger.info('Deleting updated records...')
        update_count = dest_db.execute(DEL_STMT)
        add_count = len(rows) - update_count

        logger.info('Appending new records...')
        dest_tbl.write(rows)

        # We should have added and updated at least 1 record
        if add_count == 0:
            raise HandledError('No records added')
        if update_count == 0:
            raise HandledError('No records updated')

        # Check count against Salesforce
        sf_count = sf.query_all(SF_COUNT_QUERY)['totalSize']
        db_count = dest_tbl.count()
        if sf_count != db_count:
            notes.append('Salesforce has {} rows, database has {}'\
                                    .format(sf_count, db_count))

        status = 'SUCCESS'

    except HandledError as e:
        logger.warn(e)
        notes.append(str(e))

    except Exception as e:
        import traceback
        logger.error('Unhandled error: {}'.format(traceback.format_exc()))
        notes.append('Unhandled error: ' + str(e))

    finally:
        if alerts:
            # Report to Slack
            slack_msg = '[311] {} - {}'.format(__file__, status)
            if status == 'SUCCESS':
                slack_msg += ' - {} added, {} updated'\
                                .format(add_count, update_count)
            if len(notes) > 0:
                slack_msg += ' - {}.'.format('; '.join(notes))
            if status == 'ERROR':
                slack_msg += ' @channel'
            slack.chat.post_message(SLACK_CHANNEL, slack_msg)
        logger.info('Ran successfully. Added {}, updated {}.'\
                            .format(add_count, update_count))

if __name__ == '__main__':
    sync()
