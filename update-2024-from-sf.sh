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

python sync-db2.py --prod --year_refresh 2024
