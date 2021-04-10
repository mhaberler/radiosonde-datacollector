#!/bin/bash

#set -x

# WEB=/var/www/radiosonde.mah.priv.at
# DATA=$WEB/data
# SUMMARY=$DATA/summary.geojson.br
# SUMMARY_365=$DATA/summary.year.geojson.br
# STATION_LIST=$WEB/static/station_list.json
REPO=/home/sondehub/radiosonde-datacollector-dev


. /home/sondehub/miniconda3/etc/profile.d/conda.sh

conda activate sondehub-3.8

cd $REPO

python madis-mirror.py  "$@"

if [[ $? -ne 0 ]]; then

    echo update-madis FAILED
    exit 1
fi
