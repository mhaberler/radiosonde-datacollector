import argparse
import csv
import gzip
import json
import logging
import os
import pathlib
import re
import sys
import tempfile
import time
import zipfile
from datetime import datetime
from operator import itemgetter

import brotli
import geojson

from geojsonutil import write_geojson

from bufrutil import convert_bufr_to_geojson, process_bufr

from netcdfutil import process_netcdf

# from constants import *

from config import (
    BROTLI_SUMMARY_QUALITY,
    FAILED,
    INCOMING,
    KEEP_MADIS_PROCESSED_FILES,
    MAX_ASCENT_AGE_IN_SUMMARY,
    PROCESSED,
    SPOOLDIR_GISC,
    SPOOLDIR_MADIS,
    TS_FAILED,
    TS_PROCESSED,
    TS_TIMESTAMP,
)


def gen_output(args, source, h, fn, archive, updated_stations):
    fc = convert_bufr_to_geojson(args, h)
    return write_geojson(args, source, fc, fn, archive, updated_stations)


def read_summary(fn):
    if os.path.exists(fn):
        with open(fn, "rb") as f:
            s = f.read()
            if fn.endswith(".br"):
                s = brotli.decompress(s)
            summary = geojson.loads(s.decode())
            logging.debug(f"read summary from {fn} (brotli={fn.endswith('.br')})")
    else:
        logging.debug(f"no summary file yet: {fn}")
        summary = {}
    return summary


def now():
    return datetime.utcnow().timestamp()


def update_geojson_summary(args, stations, updated_stations, summary):

    stations_with_ascents = {}
    # unroll into dicts for quick access
    if "features" in summary:
        for feature in summary.features:
            a = feature.properties["ascents"]
            if len(a):
                st_id = a[0]["station_id"]
                stations_with_ascents[st_id] = feature

    # remove entries from ascents which have a syn_timestamp less than cutoff_ts
    cutoff_ts = now() - args.max_age

    # now walk the updates
    for station, asc in updated_stations:
        if station in stations_with_ascents:

            # we already have ascents from this station.
            # append, sort by synoptic time and de-duplicate
            oldlist = stations_with_ascents[station]["properties"]["ascents"]
            oldlist.append(asc)

            pruned = [x for x in oldlist if x["syn_timestamp"] > cutoff_ts]

            logging.debug(f"pruning {station}: {len(oldlist)} -> {len(pruned)}")

            newlist = sorted(pruned, key=itemgetter("syn_timestamp"), reverse=True)
            # https://stackoverflow.com/questions/9427163/remove-duplicate-dict-in-list-in-python
            seen = set()
            dedup = []
            for d in newlist:
                # keep an ascent of each source, even if same synop time
                t = str(d["syn_timestamp"]) + d["source"]
                if t not in seen:
                    seen.add(t)
                    dedup.append(d)
            stations_with_ascents[station]["properties"]["ascents"] = dedup
        else:
            # station appears with first-time ascent
            properties = {}
            properties["ascents"] = [asc]

            if station in stations:
                st = stations[station]
                coords = (st["lon"], st["lat"], st["elevation"])
                properties["name"] = st["name"]
            else:
                # unlisted station: anonymous + mobile
                # take coords and station_id as name from ascent
                coords = (asc["lon"], asc["lat"], asc["elevation"])
                properties["name"] = asc["station_id"]

            stations_with_ascents[station] = geojson.Feature(
                geometry=geojson.Point(coords), properties=properties
            )

    # create GeoJSON summary
    ns = na = 0
    fc = geojson.FeatureCollection([])
    for _st, f in stations_with_ascents.items():
        ns += 1
        na += len(f.properties["ascents"])
        fc.features.append(f)

    gj = geojson.dumps(fc, indent=4)
    dest = os.path.splitext(args.summary)[0]
    if not dest.endswith(".br"):
        dest += ".br"

    logging.debug(f"summary {dest}: {ns} active stations, {na} ascents")

    fd, path = tempfile.mkstemp(dir=args.tmpdir)
    src = gj.encode("utf8")
    start = time.time()
    dst = brotli.compress(src, quality=BROTLI_SUMMARY_QUALITY)
    end = time.time()
    dt = end - start
    sl = len(src)
    dl = len(dst)
    ratio = (1.0 - dl / sl) * 100.0
    logging.debug(
        f"summary {dest}: brotli {sl} -> {dl}, compression={ratio:.1f}% in {dt:.3f}s"
    )
    os.write(fd, dst)
    os.fsync(fd)
    os.close(fd)
    os.rename(path, dest)
    os.chmod(dest, 0o644)


def newer(filename, ext):
    """
    given a file like foo.ext and an extension like .json,
    return True if:
        foo.json does not exist or
        foo.json has an older modification time than foo.ext
    """
    (fn, e) = os.path.splitext(filename)
    target = fn + ext
    if not os.path.exists(target):
        return True
    return os.path.getmtime(filename) > os.path.getmtime(target)


def initialize_stations(txt_fn, json_fn):
    US_STATES = [
        "AK",
        "AL",
        "AR",
        "AZ",
        "CA",
        "CO",
        "CT",
        "DE",
        "FL",
        "GA",
        "HI",
        "IA",
        "ID",
        "IL",
        "IN",
        "KS",
        "LA",
        "MA",
        "MD",
        "ME",
        "MI",
        "MN",
        "MO",
        "MS",
        "MT",
        "NC",
        "ND",
        "NE",
        "NH",
        "NJ",
        "NM",
        "NV",
        "NY",
        "OH",
        "OK",
        "OR",
        "PA",
        "RI",
        "SC",
        "SD",
        "TN",
        "TX",
        "UT",
        "VA",
        "VT",
        "WA",
        "WI",
        "WV",
        "WY",
    ]

    stationdict = {}
    with open(txt_fn, "r") as csvfile:
        stndata = csv.reader(csvfile, delimiter="\t")
        for row in stndata:
            m = re.match(
                r"(?P<stn_wmoid>^\w+)\s+(?P<stn_lat>\S+)\s+(?P<stn_lon>\S+)\s+(?P<stn_altitude>\S+)(?P<stn_name>\D+)",
                row[0],
            )
            fields = m.groupdict()
            stn_wmoid = fields["stn_wmoid"][6:]
            stn_name = fields["stn_name"].strip()

            if re.match(r"^[a-zA-Z]{2}\s", stn_name) and stn_name[:2] in US_STATES:
                stn_name = stn_name[2:].strip().title() + ", " + stn_name[:2]
            else:
                stn_name = stn_name.title()
            stn_name = fields["stn_name"].strip().title()
            stn_lat = float(fields["stn_lat"])
            stn_lon = float(fields["stn_lon"])
            stn_altitude = float(fields["stn_altitude"])

            if stn_altitude > -998.8:
                stationdict[stn_wmoid] = {
                    "name": stn_name,
                    "lat": stn_lat,
                    "lon": stn_lon,
                    "elevation": stn_altitude,
                }

        with open(json_fn, "wb") as jfile:
            j = json.dumps(stationdict, indent=4).encode("utf8")
            jfile.write(j)


def update_station_list(txt_fn):
    """
    fn is expected to look like <path>/station_list.txt
    if a corresponding <path>/station_list.json file exists and is newer:
        read that

    if the corresponding <path>/station_list.json is older or does not exist:
        read and parse the .txt file
        generate the station_list.json
        read that

    return the station fn and dict
    """
    (base, ext) = os.path.splitext(txt_fn)
    if ext != ".txt":
        raise ValueError("expecting .txt extension:", txt_fn)

    json_fn = base + ".json"

    # read the station_list.json file
    # create or update on the fly if needed from station_list.txt (same dir assumed)
    if newer(txt_fn, ".json"):
        # rebuild the json file
        logging.debug(f"rebuilding {json_fn} from {txt_fn}")

        initialize_stations(txt_fn, json_fn)
        logging.debug(f"rebuilt {json_fn} from {txt_fn}")

    with open(json_fn, "rb") as f:
        s = f.read().decode()
        stations = json.loads(s)
        logging.debug(f"read stations from {json_fn}")
    return json_fn, stations


def process_files(args, flist, station_dict, updated_stations):

    for f in flist:
        if not args.ignore_timestamps and not newer(f, TS_PROCESSED):
            logging.debug(f"skipping: {f}  (processed)")
            continue

        (fn, ext) = os.path.splitext(f)
        logging.debug(f"processing: {f} fn={fn} ext={ext}")

        if ext == ".zip":  # a zip archive of BUFR files
            try:
                with zipfile.ZipFile(f) as zf:
                    source = "gisc"
                    zip_success = True
                    for info in zf.infolist():
                        try:
                            data = zf.read(info.filename)
                            fd, path = tempfile.mkstemp(dir=args.tmpdir)
                            os.write(fd, data)
                            os.lseek(fd, 0, os.SEEK_SET)
                            file = os.fdopen(fd)
                        except KeyError:
                            logging.error(
                                f"zip file {f}: no such member {info.filename}"
                            )
                            continue
                        else:
                            logging.debug(
                                f"processing BUFR: {f} member {info.filename} size={len(data)}"
                            )
                            success, d = process_bufr(
                                args, source, file, info.filename, f
                            )
                            if success:
                                success = gen_output(
                                    args, source, d, info.filename, f, updated_stations
                                )
                            zip_success = zip_success and success
                            file.close()
                            os.remove(path)
                    if not args.ignore_timestamps:
                        gen_timestamp(fn, zip_success)

            except zipfile.BadZipFile as e:
                logging.error(f"{f}: {e}")
                if not args.ignore_timestamps:
                    gen_timestamp(fn, False)

        elif ext == ".bin":  # a singlle BUFR file
            source = "gisc"
            file = open(f, "rb")
            logging.debug(f"processing BUFR: {f}")
            success, d = process_bufr(args, source, file, f, None)
            if success:
                success = gen_output(args, source, d, fn, zip, updated_stations)

            file.close()
            if success and not args.ignore_timestamps:
                pathlib.Path(fn + ".timestamp").touch(mode=0o777, exist_ok=True)

        elif ext == ".gz":  # a gzipped netCDF file
            source = "madis"
            logging.debug(f"processing netCDF: {f}")
            try:
                success, results = process_netcdf(args, source, f, None, station_dict)

                if success:
                    for fc, file, archive in results:
                        write_geojson(args, source, fc, file, archive, updated_stations)

            except gzip.BadGzipFile as e:
                logging.error(f"{f}: {e}")

            except OSError as e:
                logging.error(f"{f}: {e}")

            if not args.ignore_timestamps:
                gen_timestamp(fn, success)


def gen_timestamp(fn, success):
    if success:
        pathlib.Path(fn + TS_PROCESSED).touch(mode=0o777, exist_ok=True)
    else:
        pathlib.Path(fn + TS_FAILED).touch(mode=0o777, exist_ok=True)


# the logging is ridiculous.
def move_files(
    directory, pattern, tsextension, destdir, keeptime=0, simulate=True, trace=False
):
    destpath = pathlib.Path(destdir)
    if not destpath.exists():
        if simulate:
            logging.debug(f"creating dir: {destpath}")
        else:
            destpath.mkdir(mode=0o755, parents=True, exist_ok=False)
    spooldir = pathlib.Path(directory)
    # if trace:
    #     logging.debug(f"spooldir={spooldir} pattern={pattern} tsextension={tsextension}")
    for path in spooldir.glob(pattern):
        # if trace:
        #     logging.debug(f"lookat: {path}")
        tspath = path.parent / pathlib.Path(path.stem + tsextension)
        # if trace:
        #     logging.debug(f"tspath: {tspath}")
        if tspath.exists():
            tssec = tspath.stat().st_mtime
            age = time.time() - tssec
            # if trace:
            #     logging.debug(f"tspath exists, age={age}: {tspath}")
            if age > keeptime:
                dpath = destpath / path.name
                dtspath = destpath / tspath.name
                if simulate:
                    logging.debug(f"time to move: {path} --> {dpath}")
                    logging.debug(f"time to move: {tspath} --> {dtspath}")
                else:
                    if trace:
                        logging.debug(f"moving: {path} --> {dpath}")
                    path.rename(dpath)
                    if trace:
                        logging.debug(f"moving: {tspath} --> {dtspath}")
                    tspath.rename(dtspath)


def keep_house(args):
    move_files(
        SPOOLDIR_MADIS + INCOMING,
        "*.gz",
        TS_PROCESSED,
        SPOOLDIR_MADIS + PROCESSED,
        keeptime=args.keep_time,
        trace=args.verbose,
        simulate=args.sim_housekeep,
    )
    move_files(
        SPOOLDIR_MADIS + INCOMING,
        "*.gz",
        TS_FAILED,
        SPOOLDIR_MADIS + FAILED,
        keeptime=0,
        trace=args.verbose,
        simulate=args.sim_housekeep,
    )

    move_files(
        SPOOLDIR_GISC + INCOMING,
        "*.zip",
        TS_FAILED,
        SPOOLDIR_GISC + FAILED,
        keeptime=0,
        trace=args.verbose,
        simulate=args.sim_housekeep,
    )
    move_files(
        SPOOLDIR_GISC + INCOMING,
        "*.zip",
        TS_TIMESTAMP,
        SPOOLDIR_GISC + PROCESSED,
        keeptime=0,
        trace=args.verbose,
        simulate=args.sim_housekeep,
    )
    move_files(
        SPOOLDIR_GISC + INCOMING,
        "*.zip",
        TS_PROCESSED,
        SPOOLDIR_GISC + PROCESSED,
        keeptime=0,
        trace=args.verbose,
        simulate=args.sim_housekeep,
    )


def main():
    parser = argparse.ArgumentParser(
        description="decode radiosonde BUFR and netCDF reports", add_help=True
    )
    parser.add_argument("-v", "--verbose", action="store_true", default=False)
    parser.add_argument(
        "--hstep",
        action="store",
        type=int,
        default=100,
        help="generate output only if samples vary vertically more than hstep",
    )
    parser.add_argument("--destdir", action="store", default=".")
    parser.add_argument(
        "--station",
        action="store",
        default=None,
        help="extract a single station by WMO id",
    )
    parser.add_argument("--geojson", action="store_true", default=False)
    parser.add_argument("--dump-geojson", action="store_true", default=False)
    parser.add_argument("--brotli", action="store_true", default=False)
    parser.add_argument(
        "--sim-housekeep",
        action="store_true",
        default=False,
        help="just list what would happen to spooldirs; no input file processing",
    )
    parser.add_argument("--only-args", action="store_true", default=False)
    parser.add_argument("--summary", action="store", required=True)
    parser.add_argument(
        "-n",
        "--ignore-timestamps",
        action="store_true",
        help="ignore, and do not create timestamps",
    )
    parser.add_argument(
        "--stations",
        action="store",
        required=True,
        help="path to station_list.txt file",
    )
    parser.add_argument("--tmpdir", action="store", default="/tmp")
    parser.add_argument(
        "--max-age", action="store", type=int, default=MAX_ASCENT_AGE_IN_SUMMARY
    )
    parser.add_argument(
        "--keep-time",
        action="store",
        type=int,
        default=KEEP_MADIS_PROCESSED_FILES,
        help="time in secs to retain processed .gz files in MADIS incoming spooldir",
    )
    parser.add_argument("files", nargs="*")

    args = parser.parse_args()
    level = logging.WARNING
    if args.verbose:
        level = logging.DEBUG

    logging.basicConfig(level=level)
    os.umask(0o22)

    station_fn, station_dict = update_station_list(args.stations)
    updated_stations = []
    summary = read_summary(args.summary)
    if not summary:
        # try brotlified version
        summary = read_summary(args.summary + ".br")

    if args.only_args:
        flist = args.files
    else:
        l = list(pathlib.Path(SPOOLDIR_GISC + INCOMING).glob("*.zip"))
        l.extend(list(pathlib.Path(SPOOLDIR_MADIS + INCOMING).glob("*.gz")))
        flist = [str(f) for f in l]

    print(flist)
    # work the backlog
    if not args.sim_housekeep:
        process_files(args, flist, station_dict, updated_stations)

    if not args.sim_housekeep and updated_stations:
        logging.debug(f"creating GeoJSON summary: {args.summary}")
        update_geojson_summary(args, station_dict, updated_stations, summary)

    if not args.only_args:
        logging.debug("running housekeeping")
        keep_house(args)


if __name__ == "__main__":
    sys.exit(main())
