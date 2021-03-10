import argparse
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
from operator import itemgetter

import geojson

from geojsonutil import write_geojson

from bufrutil import convert_bufr_to_geojson, process_bufr

from netcdfutil import process_netcdf

import pidfile

import config

import customtypes

import util


def gen_output(args, source, h, fn, archive, updated_stations):
    fc = convert_bufr_to_geojson(args, h)
    return write_geojson(args, source, fc, updated_stations)


def update_geojson_summary(args, stations, updated_stations, summary):

    stations_with_ascents = {}
    # unroll into dicts for quick access
    if "features" in summary:
        for feature in summary.features:
            a = feature.properties["ascents"]
            if len(a):
                st_id = feature.properties["station_id"]
                stations_with_ascents[st_id] = feature

    # remove entries from ascents which have a syn_timestamp less than cutoff_ts
    cutoff_ts = util.now() - args.max_age * 24 * 3600

    # now walk the updates
    for station, asc in updated_stations:
        if station in stations_with_ascents:

            # we already have ascents from this station.
            # append, sort by synoptic time and de-duplicate
            oldlist = stations_with_ascents[station]["properties"]["ascents"]
            oldlist.append(asc)

            pruned = [x for x in oldlist if x["syn_timestamp"] > cutoff_ts]


            newlist = sorted(pruned, key=itemgetter("syn_timestamp"), reverse=True)
            # https://stackoverflow.com/questions/9427163/remove-duplicate-dict-in-list-in-python
            seen = set()
            dedup = []
            for d in newlist:
                # keep an ascent of each source, even if same synop time
                t = str(d["syn_timestamp"]) + d["repfmt"]
                if t not in seen:
                    seen.add(t)
                    dedup.append(d)

            logging.debug(f"pruning {station}: {len(oldlist)} -> {len(dedup)}")
            stations_with_ascents[station]["properties"]["ascents"] = dedup

            # fixup the name if it was added to station_list.json:
            ident = stations_with_ascents[station]["properties"]["name"]
            if ident in stations:
                # using WMO id as name. Probably mobile. Replace by string name.
                stations_with_ascents[station]["properties"]["name"] = stations[ident][
                    "name"
                ]

            # overwrite the station coords by the coords of the last ascent
            # to properly handle mobile stations
            if asc["id_type"] == "mobile":
                logging.debug(
                    f"fix coords {station} -> {asc['lon']} {asc['lat']} {asc['elevation']}"
                )
                properties = stations_with_ascents[station]["properties"]
                stations_with_ascents[station] = geojson.Feature(
                    geometry=geojson.Point(
                        (
                            round(asc["lon"], 6),
                            round(asc["lat"], 6),
                            round(asc["elevation"], 1),
                        )
                    ),
                    properties=properties,
                )

        else:
            # station appears with first-time ascent
            properties = {}
            properties["ascents"] = [asc]

            if station in stations:
                st = stations[station]
                coords = (st["lon"], st["lat"], st["elevation"])
                properties["name"] = st["name"]
                properties["station_id"] = station
                properties["id_type"] = "wmo"
            else:

                # unlisted station: anonymous + mobile
                # take coords and station_id as name from ascent
                coords = (asc["lon"], asc["lat"], asc["elevation"])
                properties["name"] = asc["station_id"]

                if re.match(r"^\d{5}$", station):
                    # WMO id syntax, but not in station_list
                    # hence an unregistered but fixed station
                    properties["id_type"] = "unregistered"
                else:
                    # looks like weather ship
                    properties["id_type"] = "mobile"

            stations_with_ascents[station] = geojson.Feature(
                geometry=geojson.Point(coords), properties=properties
            )

    # create GeoJSON summary
    ns = na = 0
    fc = geojson.FeatureCollection([])
    fc.properties = {
        "fmt": config.FORMAT_VERSION,
        "generated": int(util.now()),
        "max_age": args.max_age * 24 * 3600,
    }
    for _st, f in stations_with_ascents.items():
        sid, stype = slimdown(f)
        f.properties["station_id"] = sid
        f.properties["id_type"] = stype
        ns += 1
        na += len(f.properties["ascents"])
        fc.features.append(f)

    logging.debug(f"summary {args.summary}: {ns} active stations, {na} ascents")

    useBrotli = args.summary.endswith(".br")
    util.write_json_file(fc, args.summary, useBrotli=useBrotli, asGeojson=True)


def slimdown(st):
    ascents = st.properties["ascents"]
    try:
        result = st.properties["station_id"], st.properties["id_type"]
    except KeyError:
        result = ascents[0].properties["station_id"], ascents[0].properties["id_type"]

    for a in ascents:
        a.pop("path", None)
        a.pop("path_source", None)
        a.pop("origin_member", None)
        a.pop("origin_archive", None)
        a.pop("firstSeen", None)
        a.pop("lastSeen", None)
        a.pop("fmt", None)
        a.pop("sonde_type", None)
        a.pop("sonde_serial", None)
        a.pop("sonde_humcorr", None)
        a.pop("sonde_psensor", None)
        a.pop("sonde_tsensor", None)
        a.pop("sonde_hsensor", None)
        a.pop("sonde_gepot", None)
        a.pop("sonde_track", None)
        a.pop("sonde_measure", None)
        a.pop("sonde_swversion", None)
        a.pop("sonde_frequency", None)
        a.pop("processed", None)
        a.pop("encoding", None)
#        a.pop("origin", None)
        a.pop("encoding", None)

        if st.properties["id_type"] == "wmo":
            # fixed station. Take coords from geometry.coords.
            a.pop("lat", None)
            a.pop("lon", None)
            a.pop("elevation", None)

        a.pop("station_id", None)
        a.pop("id_type", None)

    return result


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


def process_files(args, flist, updated_stations):

    for filename in flist:
        if not args.ignore_timestamps and not newer(filename, config.TS_PROCESSED):
            logging.debug(f"skipping: {filename}  (processed)")
            continue

        (fn, ext) = os.path.splitext(filename)
        logging.debug(f"processing: {filename} fn={fn} ext={ext}")

        if ext == ".zip":  # a zip archive of BUFR files
            try:
                with zipfile.ZipFile(filename) as zf:
                    source = "gisc"
                    zip_success = True
                    for info in zf.infolist():
                        try:
                            #https://docs.python.org/3/library/zipfile.html#zipinfo-objects
                            #XXX ZipInfo.date_time
                            data = zf.read(info.filename)
                            fd, path = tempfile.mkstemp(dir=config.tmpdir)
                            os.write(fd, data)
                            os.lseek(fd, 0, os.SEEK_SET)
                            infile = os.fdopen(fd)
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
                                args, source, infile, info.filename, filename
                            )
                            if success:
                                success = gen_output(
                                    args, source, d, info.filename, filename, updated_stations
                                )
                            zip_success = zip_success and success
                            infile.close()
                            os.remove(path)
                    if not args.ignore_timestamps:
                        gen_timestamp(fn, zip_success)

            except zipfile.BadZipFile as e:
                logging.error(f"{filename}: {e}")
                if not args.ignore_timestamps:
                    gen_timestamp(fn, False)

        elif (ext == ".bin") or (ext == ".bufr"):  # a singlle BUFR file
            source = "gisc"
            infile = open(filename, "rb")
            logging.debug(f"processing BUFR: {filename}")
            success, d = process_bufr(args, source, infile, filename, None)
            if success:
                success = gen_output(args, source, d, fn, None, updated_stations)

            infile.close()
            if not args.ignore_timestamps:
                gen_timestamp(fn, success)

        elif ext == ".gz":  # a gzipped netCDF file
            repfmt = "fm35" # take from channel?
            logging.debug(f"processing netCDF: {filename}")
            arrived = util.age(filename)
            with gzip.open(filename, "rb") as fd:
                try:
                    results = process_netcdf(fd,
                                             origin="gisc-foo",
                                             #filename=f,
                                             #arrived=arrived,
                                             #archive=None,
                                             pathSource="simulated",
                                             #source=source,
                                             tagSamples=config.TAG_FM35)  

                    for fc in results:
                        util.set_metadata_from_dict(fc.properties, {
                            "repfmt": repfmt,
                            "channel": "madis",
                            "archive" : None,
                            "member" : "filename",
                            "encoding": "netCDF"          
                        })
                        write_geojson(args, repfmt, fc,  updated_stations)
                        
                except gzip.BadGzipFile as e:
                    logging.error(f"{filename}: {e}")
                    if not args.ignore_timestamps:
                        gen_timestamp(fn, False)
                    
                except OSError as e:
                    logging.error(f"{filename}: {e}")
                    
                else:
                    if not args.ignore_timestamps:
                        gen_timestamp(fn, True)
                        

def gen_timestamp(fn, success):
    if success:
        pathlib.Path(fn + config.TS_PROCESSED).touch(mode=0o777, exist_ok=True)
    else:
        pathlib.Path(fn + config.TS_FAILED).touch(mode=0o777, exist_ok=True)


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
        config.SPOOLDIR_MADIS + config.INCOMING,
        "*.gz",
        config.TS_PROCESSED,
        config.SPOOLDIR_MADIS + config.PROCESSED,
        keeptime=args.keep_time,
        trace=args.verbose,
        simulate=args.sim_housekeep,
    )
    move_files(
        config.SPOOLDIR_MADIS + config.INCOMING,
        "*.gz",
        config.TS_FAILED,
        config.SPOOLDIR_MADIS + config.FAILED,
        keeptime=0,
        trace=args.verbose,
        simulate=args.sim_housekeep,
    )

    move_files(
        config.SPOOLDIR_GISC + config.INCOMING,
        "*.zip",
        config.TS_FAILED,
        config.SPOOLDIR_GISC + config.FAILED,
        keeptime=0,
        trace=args.verbose,
        simulate=args.sim_housekeep,
    )
    move_files(
        config.SPOOLDIR_GISC + config.INCOMING,
        "*.zip",
        config.TS_TIMESTAMP,
        config.SPOOLDIR_GISC + config.PROCESSED,
        keeptime=0,
        trace=args.verbose,
        simulate=args.sim_housekeep,
    )
    move_files(
        config.SPOOLDIR_GISC + config.INCOMING,
        "*.zip",
        config.TS_PROCESSED,
        config.SPOOLDIR_GISC + config.PROCESSED,
        keeptime=0,
        trace=args.verbose,
        simulate=args.sim_housekeep,
    )
    move_files(
        config.SPOOLDIR_GISC_TOKYO + config.INCOMING,
        "*.bufr",
        config.TS_FAILED,
        config.SPOOLDIR_GISC_TOKYO + config.FAILED,
        keeptime=0,
        trace=args.verbose,
        simulate=args.sim_housekeep,
    )
    move_files(
        config.SPOOLDIR_GISC_TOKYO + config.INCOMING,
        "*.bufr",
        config.TS_TIMESTAMP,
        config.SPOOLDIR_GISC_TOKYO + config.PROCESSED,
        keeptime=0,
        trace=args.verbose,
        simulate=args.sim_housekeep,
    )
    move_files(
        config.SPOOLDIR_GISC_TOKYO + config.INCOMING,
        "*.bufr",
        config.TS_PROCESSED,
        config.SPOOLDIR_GISC + config.PROCESSED,
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
        help="path to station_list.json file",
    )
    parser.add_argument("--tmpdir", action="store", default=None)
    parser.add_argument(
        "--max-age",
        action="store",
        type=int,
        default=config.MAX_DAYS_IN_SUMMARY,
        help="number of days of history to keep in summary",
    )
    parser.add_argument(
        "--keep-time",
        action="store",
        type=int,
        default=config.KEEP_MADIS_PROCESSED_FILES,
        help="time in secs to retain processed .gz files in MADIS incoming spooldir",
    )
    parser.add_argument("files", nargs="*")

    args = parser.parse_args()
    if args.tmpdir:
        config.tmpdir = args.tmpdir

    level = logging.WARNING
    if args.verbose:
        level = logging.DEBUG

    logging.basicConfig(level=level)
    os.umask(0o22)

    try:
        with pidfile.Pidfile(config.LOCKFILE, log=logging.debug, warn=logging.debug):

            config.known_stations = json.loads(util.read_file(args.stations).decode())
            updated_stations = []

            useBrotli = args.summary.endswith(".br")
            try:
                summary = util.read_json_file(
                    args.summary, useBrotli=useBrotli, asGeojson=True
                )

            except FileNotFoundError:
                summary = {}
                
            if args.only_args:
                flist = args.files
            else:
                l = list(
                    pathlib.Path(config.SPOOLDIR_GISC + config.INCOMING).glob("*.zip")
                )
                l.extend(
                    list(
                        pathlib.Path(config.SPOOLDIR_GISC_TOKYO + config.INCOMING).glob(
                            "*.bufr"
                        )
                    )
                )
                l.extend(
                    list(
                        pathlib.Path(config.SPOOLDIR_MADIS + config.INCOMING).glob(
                            "*.gz"
                        )
                    )
                )
                flist = [str(f) for f in l]

            # work the backlog
            if not args.sim_housekeep:

                process_files(args, flist, updated_stations)

            if not args.sim_housekeep and updated_stations:
                logging.debug(f"creating GeoJSON summary: {args.summary}")
                update_geojson_summary(args, config.known_stations, updated_stations, summary)

            if not args.only_args:
                logging.debug("running housekeeping")
                keep_house(args)
            return 0

    except pidfile.ProcessRunningException:
        logging.warning(f"the pid file {config.LOCKFILE} is in use, exiting.")
        return -1


if __name__ == "__main__":
    sys.exit(main())
