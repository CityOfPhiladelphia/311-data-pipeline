import sys
import csv
from datetime import datetime
# import logging
# import datum
import cx_Oracle
import petl as etl
import geopetl
from common import process_row
from config import *
from pprint import pprint

start = datetime.now()
print('Starting...')
dest_conn = cx_Oracle.connect(DEST_DB_DSN)
dest_cur = dest_conn.cursor()
# dest_db = datum.connect(DEST_DB_DSN)
# dest_table = dest_db[DEST_TABLE]

print('Dropping existing rows...')
dest_cur.execute(f'truncate table {DEST_TEMP_TABLE}')
dest_conn.commit()
# dest_table.delete()

file_path = sys.argv[1]

with open(file_path) as f:
    reader = csv.DictReader(f)

    reader_rows = []
    for r in reader:
        reader_rows.append(r)

    print('Reading...')
    # dest_rows = [process_row(row, FIELD_MAP) for row in reader_rows[5:7]]
    dest_rows = [process_row(row, FIELD_MAP) for row in reader_rows]

print('Writing...')
dest_rows.tooraclesde(DEST_TABLE)
# dest_table.write(dest_rows, chunk_size=10000)

print('Took {}'.format(datetime.now() - start))
