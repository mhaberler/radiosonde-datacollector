import argparse
import requests
import datetime
import pytz
import sys
import logging
import os
import pathlib

import config


site = "https://donneespubliques.meteofrance.fr/donnees_libres/"
prefix = "Bufr/RS_HR/"
dest = "/var/spool/meteo-fr/incoming"


def current_bufrs(dt, stations):
    if dt.hour < 12:
        valid = dt.replace(hour=0, minute=0, second=0, microsecond=0)
    else:
        valid = dt.replace(hour=12, minute=0, second=0, microsecond=0)
    vt = valid.strftime("%Y%m%d%H")
    logging.debug(f"current extension: {vt}")
    return [f"{station}.{vt}.bfr" for station in stations]


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
    args = parser.parse_args()
    level = logging.WARNING
    if args.verbose:
        level = logging.DEBUG

    logging.basicConfig(level=level)
    os.umask(0o22)

    c = config.channels["meteo-fr"]
    spool = c["spooldir"]
    stations = c["stations"]

    now = datetime.datetime.utcnow()
    filenames = current_bufrs(now, stations)
    required = looking_for(spool, filenames)

    logging.debug(f"required: {required}")
    for r in required:
        fetch(r, spool + config.INCOMING)


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
