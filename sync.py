import sys
import arrow
import click
import cx_Oracle
from simple_salesforce import Salesforce
from simple_salesforce.api import SalesforceMalformedRequest
import datum
from slacker import Slacker
from common import *
from config import *

class HandledError(Exception):
    pass

@click.command()
@click.option('--date', '-d', help='Retrieve records that were updated on a specific date (e.g. 2016-05-18). This is mostly for debugging and maintenance purposes.')
@click.option('--alerts/--no-alerts', default=True, help='Turn alerts on/off')
def sync(date, alerts):
    status = 'ERROR'
    notes = []

    try:
        print('Starting...')
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

        print('Truncating temp table...')
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
            print('Getting last updated date...')
            start_date = dest_db.execute('select max({}) from {}'\
                                        .format(DEST_UPDATED_FIELD, DEST_TABLE))[0]
            start_date = arrow.get(start_date)
            sf_query += ' AND (LastModifiedDate > {})'.format(start_date.isoformat())

        print('Fetching new records from Salesforce...')
        # print(sf_query)
        try:
            sf_rows = sf.query_all(sf_query)['records']
        except SalesforceMalformedRequest:
            raise HandledError('Could not query Salesforce')

        print('Processing rows...')
        rows = [process_row(sf_row, FIELD_MAP) for sf_row in sf_rows]

        print('Writing to temp table...')
        tmp_tbl.write(rows)

        print('Deleting updated records...')
        update_count = dest_db.execute(DEL_STMT)
        add_count = len(rows) - update_count

        # print('Appending new records...')
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
        print(e)
        notes.append(str(e))

    except Exception as e:
        print('Unhandled error')
        import traceback
        print(traceback.format_exc())
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

if __name__ == '__main__':
    sync()
