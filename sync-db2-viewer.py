import citygeo_secrets
from common import *
from config import *
import click



@click.command()
@click.option('--prod', is_flag=True)
def main(prod):
    conn = citygeo_secrets.connect_with_secrets(connect_databridge, 'databridge-v2/postgres', 'databridge-v2/hostname', 'databridge-v2/hostname-testing', prod=prod)
    try:
        # Update viewer_philly311.salesforce_cases from citygeo.salesforce_cases with
        # ONLY the rows that are missing based off the updated_datetime column
        # We could do a simple TRUNCATE and then select * to insert everything, but that's CPU intensive.
        # Only insert what we need.
        update_query = '''
        INSERT INTO viewer_philly311.salesforce_cases
        (service_request_id, status, shape, status_notes,
        service_name, service_code, agency_responsible,
        service_notice, requested_datetime, updated_datetime,
        expected_datetime, closed_datetime, address, zipcode,
        media_url, lat, lon, subject, type_, description,
        description_full, private_case, objectid)
        SELECT service_request_id, status, shape, status_notes,
        service_name, service_code, agency_responsible,
        service_notice, requested_datetime, updated_datetime,
        expected_datetime, closed_datetime, address, zipcode,
        media_url, lat, lon, subject, type_, description,
        description_full, private_case, sde.next_rowid('viewer_philly311','salesforce_cases')
        FROM citygeo.salesforce_cases rawview
        WHERE rawview.updated_datetime > (
            SELECT COALESCE(MAX(updated_datetime), '1970-01-01')
            FROM viewer_philly311.salesforce_cases
        );
        '''

        with conn.cursor() as cur: 
            cur.execute(update_query)
            conn.commit()
            # Get the number of rows affected
            print(f"Rows inserted: {cur.rowcount}")
    finally:
        # Clean up
        cur.close()
        conn.close()

if __name__ == '__main__':
    main()
