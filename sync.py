import sys
import os
from datetime import date as date_obj
import logging
import logging.handlers
import warnings
import arrow
import click
import cx_Oracle
from simple_salesforce import Salesforce
from simple_salesforce.api import SalesforceMalformedRequest
import datum
from slacker import Slacker
from common import *
from config import *

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
# # SMTP handler
logging_smtp_handler = logging.handlers.SMTPHandler(**SMTP_CONFIG)
logging_smtp_handler.setFormatter(formatter)
logging_smtp_handler.setLevel(logging.ERROR)
logger.addHandler(logging_smtp_handler)

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
            dest_db = datum.connect(DEST_DB_DSN)
            dest_tbl = dest_db[DEST_TABLE]
            tmp_tbl = dest_db[DEST_TEMP_TABLE]

            logger.info('Truncating temp table...')
            tmp_tbl.delete()

            sf_query = SF_QUERY

            # If a start date was passed in, handle it.
            if date:
                warnings.warn('Fetched records for {} only'.format(date))
                try:
                    date_comps = [int(x) for x in date.split('-')]
                    start_date = arrow.get(date_obj(*date_comps), 'US/Eastern')\
                                      .to('Etc/UTC')
                except ValueError:
                    raise HandledError('Date parameter is invalid')
                end_date = start_date.replace(days=1)

                sf_query += ' AND (LastModifiedDate >= {})'.format(start_date)
                sf_query += ' AND (LastModifiedDate < {})'.format(end_date)

            # Otherwise, grab the last updated date from the DB.
            else:
                logger.info('Getting last updated date...')
                start_date_str = dest_db.execute('select max({}) from {}'\
                                            .format(DEST_UPDATED_FIELD, DEST_TABLE))[0]
                start_date = arrow.get(start_date_str, 'US/Eastern').to('Etc/UTC')
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
            logger.info('Ran successfully. Added {}, updated {}.'\
                                    .format(add_count, update_count))

        except:
            logger.exception('Unhandled error')

        finally:
            if alerts:
                msg = '[311] {} - {}'.format(__file__, status)
                if status == 'SUCCESS':
                    msg += ' - {} added, {} updated'\
                                    .format(add_count, update_count)
                if len(w) > 0:
                    msg += ' - {}.'.format('; '.join([str(x.message) for x in w]))

                # Try to post to Slack
                try:
                    slack = Slacker(SLACK_TOKEN)
                    slack.chat.post_message(SLACK_CHANNEL, msg)
                except Exception as e:
                    logger.error(
                        'Could not post to Slack. '
                        'The message was:\n\n{}\n\n'
                        'The error was:\n\n{}'.format(msg, e)
                    )

if __name__ == '__main__':
    sync()
