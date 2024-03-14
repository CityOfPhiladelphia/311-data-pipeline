# PROD/TEST SETTINGS (either GISDPB or GISDBP_T):
TEST=False
# Logging settings.
LOG_LEVEL               = 'INFO'
LOG_PATH                = './logs/sync.log'


# These describe the destination (enterprise) dataset.
# These describe the destination (enterprise) dataset.

DEST_DB_ACCOUNT         = ''
DEST_TABLE              = 'SALESFORCE_CASES'
DEST_UPDATED_FIELD      = 'updated_datetime'
TEMP_TABLE         = 'SALESFORCE_CASES_TEMP'


# For deleting records which have since been updated.
UPDATE_COUNT_STMT = '''
    select count(*)
    from {}
    where service_request_id in
        (select service_request_id from {})
'''.format(DEST_TABLE, TEMP_TABLE)

DEL_STMT = '''
    delete from {}
    where service_request_id in
        (select service_request_id from {})
'''.format(DEST_TABLE, TEMP_TABLE)

FIELD_MAP = {
    # DESTINATION FIELD         # SOURCE FIELD
    'service_request_id':       'CaseNumber',
    'status':                   'Status',
    'service_name':             'Case_Record_Type__c',
    'service_code':             'Service_Code__c',
    'description':              'Description',
    'agency_responsible':       'Department__c',
    'service_notice':           'SLA__c',
    'requested_datetime':       'CreatedDate',
    'updated_datetime':         'LastModifiedDate',
    'expected_datetime':        'Sla_date__c',
    'address':                  'Street__c',
    'zipcode':                  'ZipCode__c',
    'media_url':                'Media_Url__c',
    'private_case':             'Private_Case__c',
    'subject':                  'Subject',
    'type_':                    'Type'
    # 'description_full':         'Description',
}

# Microsoft Teams Web Connector URL
MSTEAMS_CONNECTOR             = 'https://phila.webhook.office.com/webhookb2/763c9a83-0f38-4eb2-abfc-e0f2f41b6fbb@2046864f-68ea-497d-af34-a6629a6cd700/IncomingWebhook/434dfb8c116d472f8f224cfae367cdc1/2f82d684-85a4-4131-95a3-3342e012faeb'


# Most of the filtering for the public view we do in the database, but the
# `Type` field is not part of the schema, so we have to filter those cases
# when querying Salesforce.
SF_WHERE            = "RecordTypeId != '012G00000014BhVIAU' AND Case_Record_Type__c not in ('', 'Agency Receivables', 'Revenue Escalation') AND RecordTypeId != ''"
SF_QUERY            = '''
                        SELECT
                            CaseNumber,
                            Status,
                            Description,
                            CreatedDate,
                            LastModifiedDate,
                            Case_Record_Type__c,
                            Centerline_2272x__c,
                            Centerline_2272y__c,
                            Centerline__Latitude__s,
                            Centerline__Longitude__s,
                            Department__c,
                            Street__c,
                            Private_Case__c,
                            SLA__c,
                            Service_Code__c,
                            Resolution__c,
                            ZipCode__c,
                            Media_Url__c,
                            Sla_date__c,
                            Close_Reason__c,
                            Status_Update__c,
                            Subject,
                            Type
                        FROM Case
                        WHERE {}
                    '''.format(SF_WHERE)
SF_COUNT_QUERY      = '''
                        SELECT COUNT() FROM Case
                        WHERE {}
                    '''.format(SF_WHERE)
