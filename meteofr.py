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


def current_bufrs(day, hour, stations):
    return [f"{station}.{day}{hour}.bfr" for station in stations]


def looking_for(spool, fns):
    found = []
    for fn in fns:
        # did we received this one already?
        for subdir in [config.PROCESSED, config.INCOMING, config.FAILED]:
            pn = spool + subdir + "/" + fn
            if pathlib.Path(pn).exists():
                found.append(fn)
                break
    return list(set(fns) - set(found))


def fetch(bufr, dest):
    url = f"{site}{prefix}{bufr}"
    r = requests.get(url)
    if r:
        if "Last-Modified" in r.headers:
            logging.debug(f"retrieving: {url}, modified: {r.headers['Last-Modified']}")
            with open(f"{dest}/{bufr}", "wb") as f:
                f.write(r.content)
        else:
            logging.debug(f"not yet available: {bufr}")


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
        required=True,
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
        required=True,
        type=int,
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

    hour = f"{args.hour:02d}"
    
    c = config.channels["meteo-fr"]
    spool = c["spooldir"]

    filenames = current_bufrs(day, hour, args.stations)
    logging.debug(f"filenames: {filenames}")

    required = looking_for(spool, filenames)
    logging.debug(f"required: {required}")

    for r in required:
        fetch(r, spool + config.INCOMING)
        sleeptime = random.uniform(args.mindelay, args.maxdelay)
        logging.debug(f"sleeping for: {sleeptime:.1f} sec")
        sleep(sleeptime)


if __name__ == "__main__":
    sys.exit(main())


# check via JSON "ascents available" file:
# url =  'https://donneespubliques.meteofrance.fr/donnees_libres/Pdf/RS/RSDispos.json'
# r = requests.get(url)
# print()
# al = r.json()["Bufr/RS_HR/"]
# for s in al:
#     station = s["station"]
#     name  = s["nom"]
#     for d in  s["dates"]:
#         day = d["jour"]
#         ascents = d["reseaux"]
