cd /scripts/311-data-pipeline/
source ./env/bin/activate
python sync.py --year_refresh=2017
python sync.py --year_refresh=2018
python sync.py --year_refresh=2019
python sync.py --year_refresh=2020
python sync.py --year_refresh=2021
python sync.py --year_refresh=2022
python sync.py --year_refresh=2023
