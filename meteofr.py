import argparse
import requests
import datetime
import pytz
import sys
import logging
import os
import pathlib
from numpy import random
from time import sleep

import config


site = "https://donneespubliques.meteofrance.fr/donnees_libres/"
prefix = "Bufr/RS_HR/"
dest = "/var/spool/meteo-fr/incoming"
dispos =  'https://donneespubliques.meteofrance.fr/donnees_libres/Pdf/RS/RSDispos.json'

def current_bufrs(day, hour, stations):
    return [f"{station}.{day}{hour}.bfr" for station in stations]


def received(spool, fn):
    found = []
    # did we received this one already?
    for subdir in [config.PROCESSED, config.INCOMING, config.FAILED]:
        pn = spool + subdir + "/" + fn
        if pathlib.Path(pn).exists():
            return True
    return False

def fetch(bufr, dest, sleeptime=5, fetch=True):
    url = f"{site}{prefix}{bufr}"
    if not fetch:
        logging.debug(f"would retrieve: {url}")
        return
    r = requests.get(url)
    if r:
        if "Last-Modified" in r.headers:
            logging.debug(f"retrieving: {url} -> {dest}/{bufr}, modified: {r.headers['Last-Modified']}")
            if save:
                with open(f"{dest}/{bufr}", "wb") as f:
                    f.write(r.content)
        else:
            logging.debug(f"not yet available: {bufr}")

    logging.debug(f"sleeping for: {sleeptime:.1f} sec")
    sleep(sleeptime)

def get_missing(day, hour, url, spool):
    missing = []
    r = requests.get(url)
    if r:
       bufrs = r.json()["Bufr/RS_HR/"]
       for s in bufrs:
           station = s["station"]
           name  = s["nom"]
           for d in  s["dates"]:
               if d["jour"] == day:
                   for hr in d["reseaux"]:
                       h = int(hr)
                       if hour > h:
                           fn = f"{station}.{day}{hr}.bfr"
                           if not received(spool, fn):
                               missing.append(fn)
    return missing

def main():
    parser = argparse.ArgumentParser(
        description="fetch current BUFR files from Meteo France",
        add_help=True,
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("-v", "--verbose", action="store_true", default=False)
    parser.add_argument(
        "--dest",
        action="store",
        help="destination dir to write BUFR files to",
    )
    parser.add_argument(
        "--stations",
        nargs="+",
        type=str,
        help="station ID's to retrieve",
    )
    parser.add_argument(
        "--day",
        action="store",
        help="day tag in format YYYYMMDD, default: today",
    )
    parser.add_argument(
        "--hour",
        action="store",
        type=int,
        default=None,
        help="hour",
    )
    parser.add_argument(
        "--mindelay",
        action="store",
        default=2.0,
        type=float,
        help="min delay value",
    )
    parser.add_argument(
        "-n", "--nofetch",
        action="store_true",
        default=False,
        help="do not retrieve and save missing bufrs (debugging)",
    )
    parser.add_argument(
        "--maxdelay",
        action="store",
        default=5.0,
        type=float,
        help="max delay value",
    )
             
    args = parser.parse_args()
    level = logging.WARNING
    if args.verbose:
        level = logging.DEBUG

    logging.basicConfig(level=level)
    os.umask(0o22)

    if args.day:
        day = args.day
    else:
        day = datetime.datetime.utcnow().strftime("%Y%m%d")

    if args.hour:
        hour = args.hour
    else:
        hour = datetime.datetime.utcnow().hour

    c = config.channels["meteo-fr"]
    spool = c["spooldir"]

    missing = get_missing(day, hour, dispos, spool)
    for r in missing:
        sleeptime = random.uniform(args.mindelay, args.maxdelay)
        fetch(r, spool + config.INCOMING, sleeptime=sleeptime, fetch=not args.nofetch)


if __name__ == "__main__":
    sys.exit(main())

