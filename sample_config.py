# Logging settings (used in sync.py only).
LOG_LEVEL               = 'INFO'
LOG_PATH                = './logs/sync.log'

# These describe the destination (enterprise) dataset.
DEST_DB_DSN             = 'adapter://user:pass@db'
DEST_TABLE              = ''
DEST_UPDATED_FIELD      = ''
DEST_TEMP_TABLE         = ''

# For deleting records which have since been updated.
DEL_STMT = ''

FIELD_MAP = {
    # DESTINATION FIELD:        # SOURCE FIELD
}

# These are for querying Salesforce.
SF_USER             = ''
SF_PASSWORD         = ''
SF_TOKEN            = ''
SF_WHERE            = ''
SF_QUERY            = ''
SF_COUNT_QUERY      = ''

SLACK_TOKEN         = ''
SLACK_CHANNEL       = ''

SMTP_CONFIG          = {
                        'mailhost':     ('host', port),
                        'fromaddr':     ,
                        'toaddrs':      ,
                        'subject':      ,
                    }
