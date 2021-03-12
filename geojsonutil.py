import logging
import pathlib
from datetime import datetime
from pprint import pprint

import pytz

import config

import util

def write_geojson(destdir, repfmt, fc):

    station_id = fc.properties["station_id"]

    logging.debug(
        f"output samples retained: {len(fc.features)}, station id={station_id}"
    )

    cc = station_id[:2]
    subdir = station_id[2:5]

    syn_time = datetime.utcfromtimestamp(fc.properties["syn_timestamp"]).replace(
        tzinfo=pytz.utc
    )
    day = syn_time.strftime("%Y%m%d")
    year = syn_time.strftime("%Y")
    month = syn_time.strftime("%m")
    time = syn_time.strftime("%H%M%S")

    dest = (
        f"{destdir}/{repfmt}/{cc}/{subdir}/"
        f"{year}/{month}/{station_id}_{day}_{time}.geojson.br"
    )
    ref = f"{repfmt}/{cc}/{subdir}/" f"{year}/{month}/{station_id}_{day}_{time}.geojson"

    path = pathlib.Path(dest).parent.absolute()
    pathlib.Path(path).mkdir(parents=True, exist_ok=True)

    if not fc.is_valid:
        logging.error(f"--- invalid GeoJSON! {fc.errors()}")
        raise ValueError("invalid GeoJSON")
    #try:
    util.write_json_file(fc, dest, useBrotli=True, asGeojson=True)
    # except Exception as e:
    #     logging.exception(f"{e}")

        
    fc.properties["path"] = ref
    return True
