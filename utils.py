import cx_Oracle
from simple_salesforce import Salesforce
import citygeo_secrets
from arcgis import GIS

# Secret for sync-ago.py
def get_salesforce_ago_layer(creds:dict): 
    salesforce_creds = creds['salesforce API copy']
    ago_creds =  creds["AGO/maps.phl.backup"]
    org = GIS(url=ago_creds.get('url'),
                username=ago_creds.get('login'),
                password=ago_creds.get('password'),
                verify_cert=False)
    flayer = org.content.get(salesforce_creds.get('AGO_item_id'))
    return flayer

# Secret for sync-ago.py
def connect_sde(creds: dict):
    db_creds = creds['SDE']
    db_creds_2 = creds['databridge-oracle/hostname']
    # Connect to database
    dsn = cx_Oracle.makedsn(db_creds_2.get('host').get('hostName'), db_creds_2.get('host').get('port'),
                                   service_name=db_creds_2.get('database'))
    connection = cx_Oracle.connect(db_creds.get('login'), db_creds.get('password'), dsn,
                                          encoding="UTF-8")
    return connection

# Secret for sync.py
def connect_311(creds: dict):
    db_creds = creds['GIS_311']
    db_creds_2 = creds['databridge-oracle/hostname']
    # Connect to database
    dsn = cx_Oracle.makedsn(db_creds_2.get('host').get('hostName'), db_creds_2.get('host').get('port'),
                                   service_name=db_creds_2.get('database'))
    connection = cx_Oracle.connect(db_creds.get('login'), db_creds.get('password'), dsn,
                                          encoding="UTF-8")
    return connection

# Secret for sync.py
def connect_311_test(creds: dict):
    db_creds = creds['GIS_311']
    db_creds_2 = creds['databridge-oracle/hostname-testing']
    # Connect to database
    dsn = cx_Oracle.makedsn(db_creds_2.get('host').get('hostName'), db_creds_2.get('host').get('port'),
                                   service_name=db_creds_2.get('database'))
    connection = cx_Oracle.connect(db_creds.get('login'), db_creds.get('password'), dsn,
                                          encoding="UTF-8")
    return connection

# Secret for sync.py. SHOULD BE RENAMED TO connect_salesforce() AFTER MERGING TO MAIN
def connect_salesforce_DEV(creds:dict):
    salesforce_creds=creds['salesforce API copy']
    sf = Salesforce(username=salesforce_creds.get('login'), \
                       password=salesforce_creds.get('password'), \
                       security_token=salesforce_creds.get('token'))
    return sf
