#!/bin/bash

WEB=/var/www/yourserver.com
DATA=$WEB/data-dev
SUMMARY=$DATA/summary.geojson.br
STATION_LIST=$WEB/static/station_list.txt
SPOOL=/var/spool/gisc
INCOMING=$SPOOL/incoming
PROCESSED=$SPOOL/processed
FAILED=$SPOOL/failed
REPO=/home/radiosonde/radiosonde-deploy
TMPDIR=/tmp
FLAGS=-v
FLAGS=

. /home/radiosonde/miniconda3/etc/profile.d/conda.sh
conda activate radiosonde

cd $REPO
python process.py $FLAGS --tmpdir $TMPDIR --destdir $DATA  \
  --brotli --hstep 100 --geojson \
  --summary $SUMMARY --stations $STATION_LIST "$@"

if [[ $? -ne 0 ]]; then
    echo ingest FAILED
    exit 1
fi
