import arrow

# These agencies use different field for their status notes.
LI_STREETS_WATER = [
    'License & Inspections', 
    'Licenses & Inspections',
    'Licenses & Inspections- L&I',
    'Streets Department', 
    'Water Department (PWD)', 
]

def process_row(row, field_map):
    """
    This processes a Salesforce row. Can be either from an API call or dump 
    file.
    """
    global LI_STREETS_WATER
    
    out_row = {field: row[src_field] for field, src_field in field_map.items()}
    
    # Make geom
    shape = None
    try:
        x = float(row['Centerline__Longitude__s'])
        y = float(row['Centerline__Latitude__s'])
        if 0 not in [x, y]:
            shape = 'POINT ({} {})'.format(x, y)
    except (ValueError, TypeError):
        pass
    finally:
        out_row['shape'] = shape

    # Truncate description
    out_row['description'] = out_row['description'][:250]
    # out_row['description_full'] = out_row['description']

    # Map private flag
    private = out_row['private_case']
    private = 0 if private in [False, 'false'] else 1
    out_row['private_case'] = private

    # Datify date fields
    for date_field_prefix in ['requested', 'updated', 'expected']:
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

    # Pick source field for status notes
    if out_row['agency_responsible'] in LI_STREETS_WATER:
        status_notes = row['Resolution__c']
    else:
        if out_row['status'] == 'Closed':
            status_notes = row['Close_Reason__c']
        else:
            status_notes = row['Status_Update__c']
    out_row['status_notes'] = status_notes

    return out_row
