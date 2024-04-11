set -e

cd /scripts/311-data-pipeline/
source ./venv/bin/activate

# Get total available RAM in gigabytes
total_ram=$(free -g | awk '/^Mem:/{print $2}')

# Check if total RAM is less than 8 GB
if [ "$total_ram" -lt 7 ]; then
    echo "Insufficient RAM. Run this on an instance with at least 8 GB."
    exit 1
fi

python sync-db2.py --prod --year_refresh 2008
python sync-db2.py --prod --year_refresh 2009
python sync-db2.py --prod --year_refresh 2010
python sync-db2.py --prod --year_refresh 2011
python sync-db2.py --prod --year_refresh 2012
python sync-db2.py --prod --year_refresh 2013
python sync-db2.py --prod --year_refresh 2014
python sync-db2.py --prod --year_refresh 2015
python sync-db2.py --prod --year_refresh 2016
python sync-db2.py --prod --year_refresh 2017
python sync-db2.py --prod --year_refresh 2018
python sync-db2.py --prod --year_refresh 2019
python sync-db2.py --prod --year_refresh 2020
python sync-db2.py --prod --year_refresh 2021
python sync-db2.py --prod --year_refresh 2022
python sync-db2.py --prod --year_refresh 2023
python sync-db2.py --prod --year_refresh 2024
