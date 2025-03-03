import sys
import petl as etl
import click
from simple_salesforce import Salesforce
from requests.adapters import Retry
import citygeo_secrets
from common import *
from config import *
from databridge_etl_tools.postgres.postgres import Postgres
from datetime import datetime, timedelta
from itertools import islice
from time import sleep


def connect_to_databridge(prod):
    return citygeo_secrets.connect_with_secrets(connect_databridge, 'databridge-v2/postgres', 'databridge-v2/hostname', 'databridge-v2/hostname-testing', prod=prod)


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


def chunk_list(lst, chunk_size):
    it = iter(lst)
    while chunk := list(islice(it, chunk_size)):
        yield chunk


@click.command()
@click.option('--prod', is_flag=True)
def main(prod):
    dest_conn = connect_to_databridge(prod)
    cur = dest_conn.cursor()
    #autocommit
    dest_conn.set_session(autocommit=True)

    sf = connect_to_salesforce()

    #1. Get all service_request_id's (Known as "CaseNumber" in Salesforce)
    #service_request_ids_stmt = 'select service_request_id from viewer_philly311.salesforce_cases order by service_request_id asc;'
    service_request_ids_stmt = 'select service_request_id from viewer_philly311.salesforce_cases order by service_request_id desc;'
    #service_request_ids_stmt = 'select service_request_id from viewer_philly311.salesforce_cases limit 5;'
    with dest_conn.cursor() as cur:
        print(service_request_ids_stmt)
        cur.execute(service_request_ids_stmt)
        service_request_ids = cur.fetchall()
        service_request_ids = [x[0] for x in service_request_ids]

        print(f'Retrieved {len(service_request_ids)} records from DataBridge')

        start = datetime.now()
        count = 0

        sf_base_query = f'SELECT CaseNumber FROM Case WHERE {SF_WHERE}'
        # loop through list of service_request_ids by chunks of 1000. Much faster than one at a time.
        chunk_amount = 1000
        for CaseNumber_chunk in chunk_list(service_request_ids, chunk_amount):
            # QA check on ourselves, make sure there aren't any dupes in our own database.
            assert len(set(CaseNumber_chunk)) == chunk_amount

            # Increment our counter so the below conditional eventually triggers
            count += chunk_amount
            if count % 50000 == 0:
                print(f'At CaseNumber: {CaseNumber_chunk[0]}')
                current_duration = datetime.now() - start
                print(f'Seconds from start: {current_duration.total_seconds()}')

            # Construct a very large query selecting our chunk amount of CaseNumber's at a time. Much faster than doing 1 at a time.
            sf_existence_query = sf_base_query +  'AND CaseNumber in ('
            for i in CaseNumber_chunk:
                sf_existence_query += f" '{i}',"
            # End the query
            sf_existence_query = sf_existence_query.removesuffix(',') + ')'

            # Query salesforce.
            records = sf.query(sf_existence_query)
            
            # Compile all the returned casenumbers into a single list and then compare.
            sf_returned_cases = [ int(i['CaseNumber']) for i in records['records'] ]

            # Subtract sf returned cases from ours, so we can see what salesforce has removed.
            # don't compare the other way of course, we only care about what they don't have, not what we don't have (yet).
            deleted_cases = set(CaseNumber_chunk) - set(sf_returned_cases)

            if len(sf_returned_cases) == chunk_amount:
                #print('No deleted cases in this chunk..')
                # Give salesforce and databridge a little break, as a treat
                sleep(2)
            else:
                for d in deleted_cases:
                    print(f'{d} does not exist in SalesForce! Removing.')

                    deleted_upsert_stmt = f'''
                        INSERT INTO citygeo.salesforce_cases_deleted (
        service_request_id, status, status_notes, service_name, service_code,
        description, agency_responsible, service_notice, address, zipcode, media_url, private_case,
        description_full, subject, type_, requested_datetime, updated_datetime, expected_datetime,
        closed_datetime, gdb_geomattr_data, shape, police_district, council_district_num, pinpoint_area,
        parent_service_request_id, li_district, sanitation_district, service_request_origin, service_type,
        record_id, vehicle_model, vehicle_make, vehicle_color, vehicle_body_style, vehicle_license_plate,
        vehicle_license_plate_state
        )
        SELECT
        service_request_id, status, status_notes, service_name, service_code,
        description, agency_responsible, service_notice, address, zipcode, media_url, private_case,
        description_full, subject, type_, requested_datetime, updated_datetime, expected_datetime,
        closed_datetime, gdb_geomattr_data, shape, police_district, council_district_num, pinpoint_area,
        parent_service_request_id, li_district, sanitation_district, service_request_origin, service_type,
        record_id, vehicle_model, vehicle_make, vehicle_color, vehicle_body_style, vehicle_license_plate,
        vehicle_license_plate_state
        FROM citygeo.salesforce_cases_raw
        WHERE service_request_id = {d}
        ON CONFLICT (service_request_id)
        DO UPDATE
        SET 
        status = EXCLUDED.status,
        status_notes = EXCLUDED.status_notes,
        service_name = EXCLUDED.service_name,
        service_code = EXCLUDED.service_code,
        description = EXCLUDED.description,
        agency_responsible = EXCLUDED.agency_responsible,
        service_notice = EXCLUDED.service_notice,
        address = EXCLUDED.address,
        zipcode = EXCLUDED.zipcode,
        media_url = EXCLUDED.media_url,
        private_case = EXCLUDED.private_case,
        description_full = EXCLUDED.description_full,
        subject = EXCLUDED.subject,
        type_ = EXCLUDED.type_,
        requested_datetime = EXCLUDED.requested_datetime,
        updated_datetime = EXCLUDED.updated_datetime,
        expected_datetime = EXCLUDED.expected_datetime,
        closed_datetime = EXCLUDED.closed_datetime,
        gdb_geomattr_data = EXCLUDED.gdb_geomattr_data,
        shape = EXCLUDED.shape,
        police_district = EXCLUDED.police_district,
        council_district_num = EXCLUDED.council_district_num,
        pinpoint_area = EXCLUDED.pinpoint_area,
        parent_service_request_id = EXCLUDED.parent_service_request_id,
        li_district = EXCLUDED.li_district,
        sanitation_district = EXCLUDED.sanitation_district,
        service_request_origin = EXCLUDED.service_request_origin,
        service_type = EXCLUDED.service_type,
        record_id = EXCLUDED.record_id,
        vehicle_model = EXCLUDED.vehicle_model,
        vehicle_make = EXCLUDED.vehicle_make,
        vehicle_color = EXCLUDED.vehicle_color,
        vehicle_body_style = EXCLUDED.vehicle_body_style,
        vehicle_license_plate = EXCLUDED.vehicle_license_plate,
        vehicle_license_plate_state = EXCLUDED.vehicle_license_plate_state;
                        '''
                    
                    # Delete from the raw table.
                    del_stmt_1 = f'delete from citygeo.salesforce_cases_raw where service_request_id = {d}'
                    # Delete from the viewer table.
                    del_stmt_2 = f'delete from viewer_philly311.salesforce_cases where service_request_id = {d}'
                    
                    # upsert deleted record into our deleted table first.
                    cur.execute(deleted_upsert_stmt)
                    dest_conn.commit()
                    # Then delete it from our destination tables.
                    cur.execute(del_stmt_1)
                    cur.execute(del_stmt_2)
                    dest_conn.commit()

                    # Give salesforce and databridge a little break, as a treat
                    sleep(2)

                
    if not dest_conn.closed:
        dest_conn.close()

if __name__ == '__main__':
    main()
