#!/bin/bash


REMOTE_HOST="madis-data.ncep.noaa.gov"
REMOTE_DIR=point/raob/netcdf/
LOCAL_DIR=/var/spool/madis/incoming
USER=anonymous
PASS=user@example.com
MAIL=user@example.com

TMP=$(mktemp)
var=$(lftp -u $USER,$PASS -e "mirror --parallel=2 --verbose /$REMOTE_DIR /$LOCAL_DIR; bye" $REMOTE_HOST 2> "$TMP")
err=$(cat "$TMP")


if test -n "$err"
then
      echo "stderr=$err stdout=$var" | mail -s 'madis update failed' $MAIL
fi

rm "$TMP"
