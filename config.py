# Logging settings.
LOG_LEVEL               = 'INFO'
LOG_PATH                = './logs/sync.log'

IN_SRID = 4326
AGO_SRID = 4326

PRIMARY_KEY = 'service_request_id'

SALESFORCE_AGO_ITEMID_PROD = '3fca8f1d9a9d475a942e47fb34a85e93'
SALESFORCE_AGO_ITEMID_TEST = '3fca8f1d9a9d475a942e47fb34a85e93'

# These describe the destination (enterprise) dataset.
DEST_DB_ACCOUNT         = 'philly311'
DEST_TABLE              = 'salesforce_cases_raw'
DEST_UPDATED_FIELD      = 'updated_datetime'
TEMP_TABLE              = 'salesforce_cases_raw_temp'

# These agencies use different field for their status notes.
# Update 5/15/2024 this is no longer the case? unused
LI_STREETS_WATER = [
    'License & Inspections',
    'Licenses & Inspections',
    'Licenses & Inspections- L&I',
    'Streets Department',
    'Water Department (PWD)',
]

# TEMP
TEXT_FIELDS = [
    'status',
    'status_notes',
    'service_name',
    'service_code',
    'description',
    'agency_responsible',
    'service_notice',
    'address',
    'zipcode',
    'media_url',
    'subject',
    'type_',
]


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
    'service_request_id':          'CaseNumber',
    'status':                      'Status',
    'service_name':                'Case_Record_Type__c',
    'service_code':                'Service_Code__c',
    'description':                 'Description',
    'agency_responsible':          'Department__c',
    'service_notice':              'SLA__c',
    'requested_datetime':          'CreatedDate',
    'updated_datetime':            'LastModifiedDate',
    'expected_datetime':           'Sla_date__c',
    'closed_datetime':             'ClosedDate',
    'address':                     'Street__c',
    'zipcode':                     'ZipCode__c',
    'media_url':                   'Media_Url__c',
    'private_case':                'Private_Case__c',
    'subject':                     'Subject',
    'type_':                       'Type',
    'police_district':             'Police_District__c',
    'council_district_num':        'Council_District_No__c',
    'pinpoint_area':               'Pinpoint_Area__c',
    'parent_service_request_id':   'SAG_Parent_Case_Number__c',
    'li_district':                 'L_I_District__c',
    'sanitation_district':         'Sanitation_District__c',
    'service_request_origin':      'Origin',
    'service_type':                'Service_Request_Type__c',
    'record_id':                   'Id',
    'vehicle_model':               'Model__c',
    'vehicle_make':                'Make__c',
    'vehicle_color':               'Color__c',
    'vehicle_body_style':          'Body_Style__c',
    'vehicle_license_plate':       'License_Plate__c',
    'vehicle_license_plate_state': 'License_Plate_State__c'
}

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
                            ClosedDate,
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
                            ZipCode__c,
                            Media_Url__c,
                            Sla_date__c,
                            Close_Reason__c,
                            Status_Update__c,
                            Subject,
                            Type,
                            Police_District__c,
                            Council_District_No__c,
                            Pinpoint_Area__c,
                            SAG_Parent_Case_Number__c,
                            L_I_District__c,
                            Sanitation_District__c,
                            Origin,
                            Service_Request_Type__c,
                            Id,
                            Model__c,
                            Make__c,
                            Color__c,
                            Body_Style__c,
                            License_Plate__c,
                            License_Plate_State__c
                        FROM Case
                        WHERE {}
                    '''.format(SF_WHERE)
SF_COUNT_QUERY      = '''
                        SELECT COUNT() FROM Case
                        WHERE {}
                    '''.format(SF_WHERE)

