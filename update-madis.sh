#!/bin/bash

REMOTE_HOST="madis-data.ncep.noaa.gov"
REMOTE_DIR=point/raob/netcdf/
LOCAL_DIR=var/spool/madis/incoming
USER=anonymous
PASS=mah@mah.priv.at


(
  flock -n 9 || exit 1

  TMP=$(mktemp)
  var=$(lftp -u $USER,$PASS -e "mirror --parallel=2 --verbose /$REMOTE_DIR /$LOCAL_DIR; bye" $REMOTE_HOST 2> "$TMP")
  err=$(cat "$TMP")


  if test -z "$err"
  then
    echo madis update processed OK | logger -t processmail
  else
    echo "stderr=$err stdout=$var" | mail -s 'madis update failed' mah
    #/usr/local/bin/telegram.sh `date` mah:  madis update FAILED: $err >/dev/null
  fi
  rm -f "$TMP"

) 9>/var/lock/madis-update.sh
