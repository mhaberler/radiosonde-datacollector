import requests
import datetime
import pytz
import sys
import logging
import os
import pathlib

import config

# "07145"  Paris-Trappes
# "07110"  Brest-Guipavas
# "07761"  Ajaccio
# "07645"  Nîmes-Courbessac
# "07510"  Bordeaux-Mérignac
# "89642"  Dumont D'Urville
# "61998"  Kerguelen
# "78897"  Le Raizet
# "81405"  Rochambeau
# "61980"  Gillot
# "91925"  Hiva-Oa
# "91938"  Faa'a
# "91958"  Rapa
# "91592"  Nouméa>


site = "https://donneespubliques.meteofrance.fr/donnees_libres/"
prefix = "Bufr/RS_HR/"
dest = "/var/spool/meteo-fr/incoming"


def current_bufrs(dt, stations):
    if dt.hour < 12:
        valid = dt.replace(hour=0, minute=0, second=0, microsecond=0)
    else:
        valid = dt.replace(hour=12, minute=0, second=0, microsecond=0)
    vt = valid.strftime("%Y%m%d%H")
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


def main():
    c = config.channels["meteo-fr"]
    spool = c["spooldir"]
    stations = c["stations"]
    
    now = datetime.datetime.utcnow()
    filenames = current_bufrs(now, stations)
    required = looking_for(spool, filenames)

    print(required)


if __name__ == "__main__":
    sys.exit(main())


# for station in fr_stations:
#    print(f"{site}{prefix}{station}.{vt}.bfr")

#     now  = datetime.datetime.utcnow()
# if now.hour < 12:
#     valid =
# print("hour", now.hour)

# heute_00h = now.replace(hour=0, minute=0, second=0, microsecond=0)
# print("hour", now.hour)

# heute_00h = now.replace(hour=0, minute=0, second=0, microsecond=0)

# tag = datetime.timedelta(days=1)
# mittag = datetime.timedelta(hours=12)
# now  = datetime.datetime.utcnow()
# print("hour", now.hour)

# heute_00h = now.replace(hour=0, minute=0, second=0, microsecond=0)

# print("heute_00h", heute_00h)

# print("heute mittag", heute_00h + mittag)
