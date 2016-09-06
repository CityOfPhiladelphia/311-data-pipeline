## 311-data-pipeline

This is an ETL process for extracting and publishing data from the City of 
Philadelphia 311 system.

### Deployment

1. `git clone` this repo
2. Create a virtualenv, activate, and `pip install -r requirements.txt`
3. Rename `sample_config.py` to `config.py` and enter actual values (or download from Lastpass).
4. Create a batch file to activate the virtualenv` and `python sync.py`. Schedule this to run regularly.

### Seeding

`seed.py` is used to truncate the cases table and reload from a CSV dump. The basic usage is:

    python seed.py <file>

### Syncing

`sync.py` will check the database table for the most recent `updated_datetime` and get all records from Salesforce that have been updated since then. For a description of command-line arguments, see `python sync.py --help`.

The basic usage is:

    python sync.py

If the Salesforce query times out you may have to chunk the updates into individual days. To sync just a single day, use the `-d` option:

    python sync.py -d 2016-05-18
