import logging
import pathlib
from datetime import datetime
from pprint import pprint

import brotli

import geojson

import pytz
from config import FORMAT_VERSION


def write_geojson(args, source, fc, fn, archive, updated_stations):
    fc.properties["origin_member"] = pathlib.PurePath(fn).name
    if archive:
        fc.properties["origin_archive"] = pathlib.PurePath(archive).name
    station_id = fc.properties["station_id"]

    if args.station and args.station != station_id:
        return

    fc.properties["fmt"] = FORMAT_VERSION

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

    if args.deep:
        dest = (
            f"{args.destdir}/{source}/{cc}/{subdir}/{year}/{month}/{station_id}_{day}_{time}.geojson{cext}"
        )
        ref = f"{source}/{cc}/{subdir}/{year}/{month}/{station_id}_{day}_{time}.geojson"
    else:
        dest = (
            f"{args.destdir}/{source}/{cc}/{subdir}/{station_id}_{day}_{time}.geojson{cext}"
        )
        ref = f"{source}/{cc}/{subdir}/{station_id}_{day}_{time}.geojson"

    path = pathlib.Path(dest).parent.absolute()
    pathlib.Path(path).mkdir(parents=True, exist_ok=True)

    if not fc.is_valid:
        logging.error(f"--- invalid GeoJSON! {fc.errors()}")
        raise ValueError("invalid GeoJSON")

    try:
        gj = geojson.dumps(fc, allow_nan=False).encode("utf8")
    except ValueError as e:
        logging.error(f"dumps e={e} ref={ref}")
        raise

    logging.debug(f"writing {dest}")
    with open(dest, "wb") as gjfile:
        cmp = gj
        if args.brotli:
            cmp = brotli.compress(gj)
        gjfile.write(cmp)

    fc.properties["path"] = ref

    if args.dump_geojson:
        pprint(fc)
    return True
