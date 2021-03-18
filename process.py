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
import traceback
from operator import itemgetter
from pprint import pprint
from multiprocessing import Pool, cpu_count
from multiprocessing_logging import install_mp_handler

import geojson

from geojsonutil import write_geojson

from bufrutil import convert_bufr_to_geojson, process_bufr

from netcdfutil import process_netcdf

import pidfile

import config

import customtypes

import util

import magic

import GTStoWIS2




# for now, all netCDF files carry FM35, and BUFR files carry
# FM94 - this may or may not remain so
def filetype(s, m):
    fmt = m.id_buffer(s)
    if fmt == "NetCDF Data Format data":
        return "fm35", "netCDF"
    if fmt == "data":
        idx = s.find(config.MESSAGE_START_SIGNATURE)
        if idx == -1:
            return "unknown", "data"
        return "fm94", "BUFR"
    return "unknown", fmt

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

            newlist = sorted(pruned, key=itemgetter("syn_timestamp", "repfmt"), reverse=True)
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
            try:
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
            except Exception as e:
                print("--exception ", e)
                pprint(asc)

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
        # print(_st)
        # pprint(f)
        sid, stype = slimdown(f)
        f.properties["station_id"] = sid
        f.properties["id_type"] = stype
        ns += 1
        na += len(f.properties["ascents"])
        fc.features.append(f)

    logging.debug(f"summary {args.summary}: {ns} active stations, {na} ascents")

    useBrotli = args.summary.endswith(".br")
    util.write_json_file(fc, args.summary, useBrotli=useBrotli, asGeojson=True)

# trim summary to minimum required
def slimdown(st):
    ascents = st.properties["ascents"]
    try:
        result = st.properties["station_id"], st.properties["id_type"]
    except KeyError:
        result = ascents[0]["station_id"], ascents[0]["id_type"]

    for a in ascents:
        for k in list(a.keys()):
            if k not in ["repfmt", "syn_timestamp", "lat", "lon","elevation"]:
                a.pop(k, None)
        if st.properties["id_type"] == "wmo":
            # fixed station. Take coords from geometry.coords.
            a.pop("lat", None)
            a.pop("lon", None)
            a.pop("elevation", None)

    return result

    
def process_as(
        args, channel, repfmt, encoding, data, filename, archive, destdir, arrived, updated_stations
):

    chname = config.channels[channel]["name"]

    logging.debug(
        f"processing {repfmt}/{encoding} channel={channel} archive={archive} member={filename} size={len(data)}"
    )

    if encoding == "BUFR":
        #  e=initializer for ctype 'FILE *' must be a cdata pointer, not bytes
        fd, path = tempfile.mkstemp(dir=config.tmpdir)
        os.write(fd, data)
        os.lseek(fd, 0, os.SEEK_SET)
        infile = os.fdopen(fd)

        try:
            gts_topic = gts2wis.mapAHLtoTopic(filename)
            logging.debug(f"GTS topic for {filename}: {gts_topic}"
    )
        except Exception as e:
            gts_topic = None

        h = process_bufr(infile, filename=filename, archive=archive)
        infile.close()
        os.remove(path)
        if h == None:
            return False
        
        fc = convert_bufr_to_geojson(h, filename=filename,
                                     archive=archive,
                                     arrived=arrived,
                                     gtsTopic=gts_topic,
                                     channel=chname)
        if fc == None:
            return False
        if args.station and args.station != fc.properties["station_id"]:
            return
        if args.dump_geojson:
            pprint(fc)

        station_id = fc.properties["station_id"]
        updated_stations.append((station_id, fc.properties))
        return write_geojson(destdir, repfmt, fc)

    if encoding == "netCDF":
        results = process_netcdf(
            data,
            filename=filename,
            arrived=arrived,
            pathSource="simulated",
            tagSamples=config.TAG_FM35,
        )

        success = True

        for fc in results:
            util.set_metadata_from_dict(
                fc.properties,
                {
                    "repfmt": repfmt,
                    "channel": chname,
                    "encoding": "netCDF",
                    "id_type": "wmo"
                },
            )
            station_id = fc.properties["station_id"]
            updated_stations.append((station_id, fc.properties))

        args =  [(destdir, repfmt, f) for f in results]
        r = pool.starmap(write_geojson, args)
        logging.debug(f'{len(args)} jobs finished, success={not False in r}')
        success = not False in r
            
        return success

    logging.error(f"{archive}:{filename} : unknown file type {encoding}")


def process_files(args, wdict, updated_stations):
    with magic.Magic() as magique:
        for chan, flist in wdict.items():
            # chan = "gisc-offenbach" ..

            for filename in flist:
                if not args.ignore_timestamps and not util.newer(
                    filename, config.TS_PROCESSED
                ):
                    logging.debug(f"skipping: {filename}  (processed)")
                    continue

                arrived = int(util.age(filename))

                (fn, ext) = os.path.splitext(filename)
                #logging.debug(f"processing: {filename} fn={fn} ext={ext}")

                if ext == ".zip":
                    logging.debug(f"processing zip archive: {filename} fn={fn} ext={ext}")

                    try:
                        with zipfile.ZipFile(filename) as zf:
                            zip_success = True
                            for info in zf.infolist():
                                try:
                                    # https://docs.python.org/3/library/zipfile.html#zipinfo-objects
                                    # XXX ZipInfo.date_time
                                    data = zf.read(info.filename)

                                except KeyError:
                                    logging.error(
                                        f"zip file {f}: no such member {info.filename}"
                                    )
                                    continue
                                else:
                                    repfmt, encoding = filetype(data, magique)
                                    if repfmt not in ["fm35", "fm94"]:
                                        logging.debug(f"skipping member: {info.filename} repfmt={repfmt} encoding={encoding}")
                                        continue
                                    
                                    success = process_as(
                                        args,
                                        chan,
                                        repfmt,
                                        encoding,
                                        data,
                                        info.filename,
                                        pathlib.Path(filename).name,
                                        args.destdir,
                                        arrived,
                                        updated_stations,
                                    ) 

                                    zip_success = zip_success and success

                            if not args.ignore_timestamps:
                                gen_timestamp(fn, zip_success)

                    except zipfile.BadZipFile as e:
                        logging.error(f"{filename}: {e}")
                        if not args.ignore_timestamps:
                            gen_timestamp(fn, False)

                else:
                    # plain files
                    if ext.endswith("gz"):
                        open_method = gzip.open
                    else:
                        open_method = open

                    with open_method(filename, "rb") as fd:

                        try:
                            data = fd.read()
                            repfmt, encoding = filetype(data, magique)
                            success = process_as(
                                args,
                                chan,
                                repfmt,
                                encoding,
                                data,
                                pathlib.Path(filename).name,
                                None,
                                args.destdir,
                                arrived,
                                updated_stations,
                            )

                        except gzip.BadGzipFile as e:
                            logging.error(f"{filename}: {e}")
                            if not args.ignore_timestamps:
                                gen_timestamp(fn, False)

                        except OSError as e:
                            logging.error(f"{filename}: {e}")

                        else:
                            if not args.ignore_timestamps:
                                gen_timestamp(fn, success)


def gen_timestamp(fn, success):
    if success:
        pathlib.Path(fn + config.TS_PROCESSED).touch(mode=0o777, exist_ok=True)
    else:
        pathlib.Path(fn + config.TS_FAILED).touch(mode=0o777, exist_ok=True)


def build_work_items(arg_channels):
    if len(arg_channels) == 0:
        chan = config.channels
    else:
        for c in arg_channels:
            if c not in config.channels:
                logging.error(
                    (
                        f"no such channel: {c} -"
                        f" valid channels are: {config.channels.keys()}"
                    )
                )
                return {}
        chan = arg_channels

    fdict = {}
    for c in chan:
        d = config.channels[c]
        g = [str(f) for f in list(pathlib.Path(d["spooldir"] + config.INCOMING + "/").glob("*"))]
        m = re.compile(d["pattern"])
        files = [f for f in g if m.search(f)]
        fdict[c] = [str(f) for f in files]
                       
    return fdict


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

    if not spooldir.exists():
        if simulate:
            logging.debug(f"creating dir: {spooldir}")
        else:
            spooldir.mkdir(mode=0o755, parents=True, exist_ok=False)
    
    # if trace:
    #     logging.debug(f"spooldir={spooldir} pattern={pattern} tsextension={tsextension}")

    m = re.compile(pattern)
    for path in spooldir.glob("*"):
        if not m.search(str(path)):
            continue
        
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

def remove_files(spooldir, retain, subdirs, simulate=True):
    
    now = util.now()
    for s in subdirs:
        sd = spooldir + s
        for f in  pathlib.Path(sd).glob("*"):
            secs =  now - util.age(f) 
            if secs > retain * 86400:
                logging.debug(f"removing: {f} age={secs/86400:.1f} days")
                if not simulate:
                    try:
                        f.unlink()
                    except Exception as e:
                        logging.error(f"failed to unlink {f}: {e}")


def keep_house(args):

    for chan, desc in config.channels.items():
        spooldir = desc["spooldir"]
        pattern = desc["pattern"]
        keeptime = desc["keeptime"]
        retain = desc["retain"]
        if keeptime: # madis special case
            keeptime=args.keep_time

        logging.debug(f"janitor at: {chan} retain={retain}")

        move_files(spooldir + config.INCOMING,
                   pattern,
                   config.TS_PROCESSED,
                   spooldir + config.PROCESSED,
                   keeptime=keeptime,
                   trace=args.verbose,
                   simulate=args.sim_housekeep)
        move_files(spooldir + config.INCOMING,
                   pattern,
                   config.TS_FAILED,
                   spooldir + config.FAILED,
                   keeptime=0,
                   trace=args.verbose,
                   simulate=args.sim_housekeep)

        if args.clean_spool:
            remove_files(spooldir, retain,
                         [config.INCOMING,
                          config.PROCESSED,
                          config.FAILED],
                         simulate=args.sim_housekeep)


def main():
    parser = argparse.ArgumentParser(
        description="decode radiosonde BUFR and netCDF reports", add_help=True
    )
    parser.add_argument("-v", "--verbose", action="store_true", default=False)
    parser.add_argument("-c", "--clean-spool",
                        action="store_true",
                        help="remove overage files from spool directories",
                        default=False)
    parser.add_argument(
        "--hstep",
        action="store",
        type=int,
        default=None,
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
    parser.add_argument("--firstonly", action="store_true", default=False)
    parser.add_argument("-D", "--dump-geojson", action="store_true", default=False)
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
    parser.add_argument(
        "--channels",
        nargs="+",
        type=str,
        default=[],
        help="run named channels instead of all (default)",
    )
    parser.add_argument("files", nargs="*")

    args = parser.parse_args()
    if args.tmpdir:
        config.tmpdir = args.tmpdir

    if args.hstep:
        config.HSTEP = args.hstep

    level = logging.WARNING
    if args.verbose:
        level = logging.DEBUG

    logging.basicConfig(level=level)
    install_mp_handler()
    os.umask(0o22)

    global pool
    global gts2wis
    gts2wis = GTStoWIS2.GTStoWIS2(debug=False, dump_tables=False)
    
    try:
        with pidfile.Pidfile(config.LOCKFILE + pathlib.Path(args.destdir).name + ".pid",
                             log=logging.debug,
                             warn=logging.debug) as pf, Pool(cpu_count()) as pool:

            config.known_stations = json.loads(util.read_file(args.stations).decode())
            updated_stations = []

            try:
                summary = util.read_json_file(
                    args.summary, useBrotli=args.summary.endswith(".br"), asGeojson=True
                )
            except FileNotFoundError:
                summary = {}

            if args.only_args:
                if len(args.channels) != 1:
                    logging.error(
                        f"a single channel must be named to process {args.files}"
                    )
                    raise Exception("bad arguments")

                wdict = {args.channels[0]: args.files}
            else:
                wdict = build_work_items(args.channels)

            # work the backlog
            if not args.sim_housekeep:

                process_files(args, wdict, updated_stations)

            if not args.sim_housekeep and updated_stations:
                logging.debug(f"creating GeoJSON summary: {args.summary}")
                #pprint(updated_stations)
                update_geojson_summary(
                    args, config.known_stations, updated_stations, summary
                )

            if not args.only_args:
                logging.debug("running housekeeping")
                keep_house(args)
            return 0

    except Exception as e:
        logging.exception(f"{e}")
        return -2

    except pidfile.ProcessRunningException:
        logging.error(f"the pid file {config.LOCKFILE} is in use, exiting.")
        return -1


if __name__ == "__main__":
    sys.exit(main())
