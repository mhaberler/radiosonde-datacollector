import logging
import pathlib
from datetime import datetime
from pprint import pprint

import pytz

import config

import util

def write_geojson(args, source, fc, fn, archive, updated_stations):
    fc.properties["processed"] = int(datetime.utcnow().timestamp())
    fc.properties["origin_member"] = pathlib.PurePath(fn).name
    if archive:
        fc.properties["origin_archive"] = pathlib.PurePath(archive).name
    station_id = fc.properties["station_id"]

    if args.station and args.station != station_id:
        return

    fc.properties["fmt"] = config.FORMAT_VERSION

    logging.debug(
        f"output samples retained: {len(fc.features)}, station id={station_id}"
    )

    updated_stations.append((station_id, fc.properties))

    cext = ""
    if args.brotli:
        cext = ".br"

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
        f"{args.destdir}/{source}/{cc}/{subdir}/"
        f"{year}/{month}/{station_id}_{day}_{time}.geojson{cext}"
    )
    ref = f"{source}/{cc}/{subdir}/" f"{year}/{month}/{station_id}_{day}_{time}.geojson"

    path = pathlib.Path(dest).parent.absolute()
    pathlib.Path(path).mkdir(parents=True, exist_ok=True)

    if not fc.is_valid:
        logging.error(f"--- invalid GeoJSON! {fc.errors()}")
        raise ValueError("invalid GeoJSON")

    util.write_json_file(fc, dest, useBrotli=True, asGeojson=True)

    fc.properties["path"] = ref

    if args.dump_geojson:
        pprint(fc)
    return True
