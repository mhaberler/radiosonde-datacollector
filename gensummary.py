import argparse
import csv
import pathlib
import geojson
import sys
import os
import logging
import ciso8601
import json
from geojson import Feature, Point
import re
from operator import itemgetter
import reverse_geocoder as rg

import pidfile

import config

import util

"""
generate summary JSON file of all files under MADIS and GISC

update the station_list.json from station_list.txt if older
read the station_list.json file

read the file tree of *.geojson.br files
reconstruct summary
compress and write

"""


flights = {}
missing = {}
txtfrag = []


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
        stndata = csv.reader(filter(lambda row: row[0]!='#', csvfile) , delimiter="\t")
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
        util.write_json_file(stationdict, json_fn)


def update_station_list(txt, jsn):

    if util.age(txt) < util.age(jsn):
        return

    # rebuild the json file
    initialize_stations(txt, jsn)
    logging.debug(f"rebuilt {jsn} from {txt}")


def walkt_tree(toplevel, directory, pattern, after):
    nf = 0
    for p in sorted(directory.rglob(pattern)):
        s = p.stem
        if s.endswith(".geojson"):
            s = s.rsplit(".", 1)[0]
        stid, day, tim = s.split("_")
        ts = ciso8601.parse_datetime(day + " " + tim + "-00:00").timestamp()

        if ts < after:
            # print("skipping", s, file=sys.stderr)
            continue

        # print(stid, day, tim, datetime.fromtimestamp(ts, pytz.utc))
        if toplevel.endswith("fm35/"):
            typus = "fm35"
        if toplevel.endswith("fm94/"):
            typus = "fm94"
        entry = {"repfmt": typus, "syn_timestamp": int(ts)}
        gj = None
        if stid not in station_list:
            # maybe mobile. Check ascent for type
            # example unregistered, but obviously fixed:
            # https://radiosonde.mah.priv.at/data-dev/gisc/08/383/08383_20210206_120000.geojson
            # check id syntax - 5 digits = unregistered else mobile
            if re.match(r"^\d{5}$", stid):
                # WMO id syntax, but not in station_list
                # hence an unregistered but fixed station
                idtype = "unregistered"
            else:
                # could be ship registration syntax. Check detail file.
                gj = util.read_json_file(p, asGeojson=True, useBrotli=True)
                idtype = gj.properties["id_type"]
                # propagate per-ascent coords down to ascent
                entry["lat"] = round(gj.properties["lat"], 6)
                entry["lon"] = round(gj.properties["lon"], 6)
                entry["elevation"] = round(gj.properties["elevation"], 2)
        else:
            # registered
            st = station_list[stid]
            idtype = "wmo"

        if stid not in flights:
            flights[stid] = Feature(
                # FIXME add point after sorting for mobiles
                properties={"ascents": [entry]}
            )
        else:
            flights[stid].properties["ascents"].append(entry)

        f = flights[stid]
        f.properties["station_id"] = stid
        f.properties["id_type"] = idtype
        f.properties["name"] = stid

        if stid in station_list:
            st = station_list[stid]
            # override name if we have one
            f.properties["name"] = st["name"]
        else:
            if idtype == "unregistered":
                if stid not in missing:
                    locations = rg.search((st["lat"], st["lon"]), verbose=False)
                    if locations:
                        print(
                            stid,
                            locations,
                            st["lat"],
                            st["lon"],
                            st["elevation"],
                            file=sys.stderr,
                        )
                        loc = locations[0]
                        f.properties["name"] = loc["name"] + ", " + loc["cc"]
                        s = f'{stid.rjust(11, "X")} {st["lat"]} {st["lon"]} {st["elevation"]} {f.properties["name"]} 2020'
                        txtfrag.append(s)
                    missing[stid] = {
                        "name": f.properties["name"],
                        "lat": st["lat"],
                        "lon": st["lon"],
                        "elevation": st["elevation"],
                    }
        # this needs fixing up for mobiles after sorting
        f.geometry = Point(
            (round(st["lon"], 6), round(st["lat"], 6), round(st["elevation"], 1))
        )
        nf += 1
    return (nf, 1, 1)


def fixup_flights(flights):
    # pass 1: reverse sort ascents by timestamp
    for _stid, f in flights.items():
        a = f.properties["ascents"]
        f.properties["ascents"] = sorted(
            a, key=itemgetter("syn_timestamp"), reverse=True
        )

    # pass 2: for mobile stations, propagate up
    # coords of newest ascent to geometry.coords
    for _stid, f in flights.items():
        if f.properties["id_type"] == "mobile":
            latest = f.properties["ascents"][0]
            f.geometry = Point(
                (
                    round(latest["lon"], 6),
                    round(latest["lat"], 6),
                    round(latest["elevation"], 1),
                )
            )


def main():
    parser = argparse.ArgumentParser(
        description="rebuild radiosonde summary.json",
        add_help=True,
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("-v", "--verbose", action="store_true", default=False)
    parser.add_argument(
        "--station-json",
        action="store",
        default=config.STATION_LIST,
        help="path to write the station_list.json file",
    )
    parser.add_argument(
        "--station-text",
        action="store",
        default=config.STATION_TXT,
        help="path to the source text file to generate the station_list.json",
    )

    parser.add_argument(
        "--summary",
        action="store",
        #default=config.WWW_DIR + config.DATA_DIR + config.SUMMARY,
        default=config.SUMMARY,
        help="path of brotli-compressed summary.geojson.br",
    )

    parser.add_argument(
        "--dirs",
        nargs="+",
        type=str,
        default=[config.FM35_DATA, config.FM94_DATA],
        help="directories to scan for detail files (*.geojson.br)",
    )
    parser.add_argument(
        "--max-age",
        action="store",
        type=int,
        default=config.MAX_DAYS_IN_SUMMARY,
        help="number of days of history to keep in summary",
    )
    parser.add_argument("--tmpdir", action="store", default=None)

    args = parser.parse_args()
    if args.tmpdir:
        config.tmpdir = args.tmpdir
    level = logging.WARNING
    if args.verbose:
        level = logging.DEBUG

    logging.basicConfig(level=level)
    os.umask(0o22)

    if not os.path.exists(args.station_text):
        logging.error(f"the {args.station_text} does not exist")
        sys.exit(1)

    for d in args.dirs:
        if not os.path.exists(d):
            logging.error(f"the directory {d} does not exist")
            sys.exit(1)

    try:
        with pidfile.Pidfile(config.LOCKFILE, log=logging.debug, warn=logging.debug):

            cutoff_ts = util.now() - args.max_age * 24 * 3600
            update_station_list(args.station_text, args.station_json)

            global station_list
            station_list = json.loads(util.read_file(args.station_json).decode())
            ntotal = 0
            for d in args.dirs:
                nf, nu, nc = walkt_tree(d, pathlib.Path(d), "*.geojson.br", cutoff_ts)
                ntotal = ntotal + nf

            fixup_flights(flights)
            fc = geojson.FeatureCollection([])
            fc.properties = {
                "fmt": config.FORMAT_VERSION,
                "generated": int(util.now()),
                "max_age": config.MAX_DAYS_IN_SUMMARY * 24 * 3600,
            }
            for _st, f in flights.items():
                fc.features.append(f)

            util.write_json_file(fc, args.summary, useBrotli=True, asGeojson=True)

            for l in txtfrag:
                print(l, file=sys.stderr)

    except pidfile.ProcessRunningException:
        logging.warning(f"the pid file {config.LOCKFILE}is in use, exiting.")
        return -1


if __name__ == "__main__":

    sys.exit(main())
