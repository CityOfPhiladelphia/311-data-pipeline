#!/bin/bash
## Exit entire script if ctrl+c
trap "echo; exit" INT

cd /scripts/311-data-pipeline/
source ./venv/bin/activate
python sync-db2.py --year_refresh=2008 --date_column=CreatedDate --prod
python sync-db2.py --year_refresh=2009 --date_column=CreatedDate --prod
python sync-db2.py --year_refresh=2010 --date_column=CreatedDate --prod
python sync-db2.py --year_refresh=2011 --date_column=CreatedDate --prod
python sync-db2.py --year_refresh=2012 --date_column=CreatedDate --prod
python sync-db2.py --year_refresh=2013 --date_column=CreatedDate --prod
python sync-db2.py --year_refresh=2014 --date_column=CreatedDate --prod
python sync-db2.py --year_refresh=2015 --date_column=CreatedDate --prod
python sync-db2.py --year_refresh=2016 --date_column=CreatedDate --prod
python sync-db2.py --year_refresh=2017 --date_column=CreatedDate --prod
python sync-db2.py --year_refresh=2018 --date_column=CreatedDate --prod
python sync-db2.py --year_refresh=2019 --date_column=CreatedDate --prod
python sync-db2.py --year_refresh=2020 --date_column=CreatedDate --prod
python sync-db2.py --year_refresh=2021 --date_column=CreatedDate --prod
python sync-db2.py --year_refresh=2022 --date_column=CreatedDate --prod
python sync-db2.py --year_refresh=2023 --date_column=CreatedDate --prod
python sync-db2.py --year_refresh=2024 --date_column=CreatedDate --prod
