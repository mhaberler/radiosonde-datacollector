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


def main():
    parser = argparse.ArgumentParser(
        description="rebuild station_list.json",
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

    args = parser.parse_args()

    level = logging.WARNING
    if args.verbose:
        level = logging.DEBUG

    logging.basicConfig(level=level)
    os.umask(0o22)

    if not os.path.exists(args.station_text):
        logging.error(f"the {args.station_text} does not exist")
        sys.exit(1)


    try:
        with pidfile.Pidfile(config.LOCKFILE, log=logging.debug, warn=logging.debug):

            update_station_list(args.station_text, args.station_json)

    except pidfile.ProcessRunningException:
        logging.warning(f"the pid file {config.LOCKFILE}is in use, exiting.")
        return -1


if __name__ == "__main__":

    sys.exit(main())
