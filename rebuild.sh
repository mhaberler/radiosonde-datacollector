#!/bin/bash

WEB=/var/www/radiosonde.mah.priv.at
DATA=$WEB/data-dev
SUMMARY=$DATA/summary.geojson.br
STATION_LIST=$WEB/static/station_list.txt
SPOOL=/var/spool/gisc
INCOMING=$SPOOL/incoming
PROCESSED=$SPOOL/processed
FAILED=$SPOOL/failed
REPO=/home/sondehub/radiosonde-datacollector-master
FLAGS=-v
FLAGS=

. /home/sondehub/miniconda3/etc/profile.d/conda.sh

conda activate sondehub-3.8

cd $REPO
python process.py $FLAGS --tmpdir /junk/tmp --destdir $DATA  \
  --brotli --hstep 100 --geojson \
  --summary $SUMMARY --stations $STATION_LIST "$@"

if [[ $? -ne 0 ]]; then
    echo rebuild sondhub FAILED
    exit 1
fi
