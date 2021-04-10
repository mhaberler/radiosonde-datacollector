#!/bin/bash

#set -x

WEB=/var/www/radiosonde.mah.priv.at
DATA=$WEB/data
SUMMARY=$DATA/summary.geojson.br
SUMMARY_365=$DATA/summary.year.geojson.br
STATION_LIST=$WEB/static/station_list.json
REPO=/home/sondehub/radiosonde-datacollector-dev


. /home/sondehub/miniconda3/etc/profile.d/conda.sh

conda activate sondehub-3.8

cd $REPO

python update-stations.py

python gensummary.py  \
       --tmpdir /junk/tmp/ \
       --summary $SUMMARY \
       --station-json $STATION_LIST "$@"
if [[ $? -ne 0 ]]; then

    echo gensummary 14 days FAILED
    exit 1
fi

python gensummary.py  \
       --max-age 365 \
       --tmpdir /junk/tmp/ \
       --summary $SUMMARY_365 \
       --station-json $STATION_LIST "$@"
if [[ $? -ne 0 ]]; then

    echo gensummary year FAILED
    exit 1
fi
