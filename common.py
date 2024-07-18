import arrow
import psycopg2
import os
from config import *
from databridge_etl_tools.postgres.postgres import Postgres, Postgres_Connector
from arcgis import GIS
import re
import unicodedata

# Setup global database vars/objects to be used between our two functions below.
def connect_databridge(creds: dict, prod):
    db2_creds = creds['databridge-v2/philly311']
    if prod:
        print('Connecting to PROD databridge database!')
        host = creds['databridge-v2/hostname']['host']
    else:
        print('Connecting to test databridge database.')
        host = creds['databridge-v2/hostname-testing']['host']

    conn = psycopg2.connect(f"user={db2_creds['login']} password={db2_creds['password']} host={host} dbname=databridge")
    return conn

def create_dbtools_connector(creds: dict, prod):
    # Makes a connector object with databridge_etl_tools.postgres.postgres.Postgres_Connector
    # for use with the databridge_etl_tools load function
    db2_creds = creds['databridge-v2/philly311']
    if prod:
        print('Connecting to PROD databridge database!')
        host = creds['databridge-v2/hostname']['host']
    else:
        print('Connecting to test databridge database.')
        host = creds['databridge-v2/hostname-testing']['host']

    # confirm login works
    conn = psycopg2.connect(f"user={db2_creds['login']} password={db2_creds['password']} host={host} dbname=databridge")
    # return dbtools connector object
    connector = Postgres_Connector(connection_string=f"postgresql://{db2_creds['login']}:{db2_creds['password']}@{host}:5432/databridge")
    return connector


import boto3
import citygeo_secrets


def connect_aws_s3(creds: dict):
    aws_creds = creds['Citygeo AWS Key Pair PROD']
    # Create an IAM client
    iam_client = boto3.client('iam',
        aws_access_key_id       = aws_creds['access_key'],
        aws_secret_access_key   = aws_creds['secret_key'],
        region_name             = aws_creds['region']
        )

    # Get user information (this is a simple IAM operation)
    user_info = iam_client.get_user()

    # If the operation was successful, the key pair is valid
    print("AWS key pair is valid.")
    print('Connected UserName: ' + user_info['User']['UserName'])

    # Export for usage by dbtools
    os.environ['AWS_ACCESS_KEY_ID'] = aws_creds['access_key']
    os.environ['AWS_SECRET_ACCESS_KEY'] = aws_creds['secret_key']
    
    # Now connect to another service and return the object
    s3 = boto3.client('s3',
        aws_access_key_id       = aws_creds['access_key'],
        aws_secret_access_key   = aws_creds['secret_key'],
        region_name             = aws_creds['region']
        )
    return s3



def connect_ago_token(creds: dict):
    creds = creds['maps.phl.data']
    token_url = 'https://arcgis.com/sharing/rest/generateToken'
    data = {'username': creds['login'],
            'password': creds['password'],
            'referer': 'https://www.arcgis.com',
            'f': 'json'}
    try:
        ago_token = requests.post(token_url, data).json()['token']
    except KeyError as e:
        raise Exception('AGO login failed!')
    return ago_token

def connect_ago_arcgis(creds: dict):
    # Assume first item passed to us is the user specific creds
    first = next(iter(creds))
    creds = creds[first]

    org = GIS(url='https://phl.maps.arcgis.com',
                username=creds['login'],
                password=creds['password'],
                verify_cert=False)
    return org

def connect_salesforce(creds:dict):
    d=creds['salesforce API copy']
    return d

def process_row(row, field_map):
    """
    This processes a Salesforce row. Can be either from an API call or dump
    file.
    """
    out_row = {field: row[src_field] for field, src_field in field_map.items()}
    # Make geom
    shape = None
    try:
        x = float(row['Centerline__Longitude__s'])
        y = float(row['Centerline__Latitude__s'])
        if 0 not in [x, y]:
            shape = 'SRID=4326;POINT ({} {})'.format(x, y) if x else 'POINT EMPTY'
    except (ValueError, TypeError):
        pass
    finally:
        out_row['shape'] = shape

    # Truncate description and description_full
    # also remove bad characters before making description_full
    # If we don't remove odd characters like emojis I believe they get expanded into more characters.
    try:
        out_row['description'] = out_row['description'].strip('<>\'')
        out_row['description'] = unicodedata.normalize("NFKD", out_row['description']).encode("ascii", "ignore").decode() 
        out_row['description_full'] = out_row['description'][:2000]
        out_row['description'] = out_row['description'][:250]
    except:
        pass

    # Truncate this dumb field because people put whatever in here:
    try:
        out_row['vehicle_license_plate_state'] = out_row['vehicle_license_plate_state'][:30]
    except:
        pass

    # Clean police_district
    try:
        match = re.findall(r'\d+', out_row['police_district'])
        out_row['police_district'] = int(match[0]) if match else None
    except:
        out_row['police_district'] = None
    # Discard values greater than 100, bad input.
    if out_row['police_district']:
        if int(out_row['police_district']) > 100:
            print(f"Bad police_district input, discarding: {out_row['police_district']}")
            out_row['police_district'] = None

    # Clean council_district_num
    try:
        match = re.findall(r'\d+', out_row['council_district_num'])
        out_row['council_district_num'] = int(match[0]) if match else None
    except:
        out_row['council_district_num'] = None
    # Discard values greater than 100, bad input.
    if out_row['council_district_num']:
        if int(out_row['council_district_num']) > 100:
            print(f"Bad council_district_num input, discarding: {out_row['police_district']}")
            out_row['council_district_num'] = None

    # Lowercase pinpoint_area
    try:
        out_row['pinpoint_area'] = out_row['pinpoint_area'].lower().strip()
    except:
        out_row['pinpoint_area'] = None

    # int parent_service_request_id (SAG_Parent_Case_Number__c)
    try:
        out_row['parent_service_request_id'] = int(out_row['parent_service_request_id']) if out_row['parent_service_request_id'] != 0 and out_row['parent_service_request_id'] != '0' else None
    except:
        out_row['parent_service_request_id'] = None

    # Map private flag
    private = out_row['private_case']
    private = 0 if private in [False, 'false'] else 1
    out_row['private_case'] = private

    # Datify date fields
    for date_field_prefix in ['requested', 'updated', 'expected','closed']:
        field = date_field_prefix + '_datetime'
        val = out_row[field]
        try:
            # Make Arrow object
            a = arrow.get(val)
            # Convert to local time
            a_local = a.to('US/Eastern')
            out_row[field] = a_local.datetime
        except arrow.parser.ParserError:
            out_row[field] = None
        except TypeError:
            out_row[field] = None

    # Pick source field for status notes
    if out_row['status'] == 'Closed':
        status_notes = row['Close_Reason__c']
    else:
        status_notes = row['Status_Update__c']

    # Clean status_notes as it can take arbitrary user input.
    if isinstance(status_notes, str):
        status_notes = status_notes.strip('<>\'')
        status_notes = unicodedata.normalize("NFKD", status_notes).encode("ascii", "ignore").decode() 
        if len(status_notes) > 2000:
            status_notes = status_notes[:2000]

    out_row['status_notes'] = status_notes

    # TEMP: check for excessively long strings until this is
    # implemented in Datum.
    global TEXT_FIELDS
    for text_field in TEXT_FIELDS:
        out_row[text_field] = (out_row[text_field] or '')[:2000]

    return out_row
