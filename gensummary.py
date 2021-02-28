import pathlib
import brotli
import geojson
import time
import sys
import os
import ciso8601
from datetime import datetime
import pytz
import json
from config import STATION_LIST
from geojson import Feature, FeatureCollection, Point
from pprint import pprint
import brotli
'''

generate summary JSON file of all files under MADIS and GISC

read the station_list.json file

read the file tree of *.geojson.br files
split filename into station_id, day, time
construct timestamp

add to ascents[station_id]:
    if station_id in ascents:
        append to st.ascents{ts, type=X/Y}
    else
        create entry
            take coord,name from station_list.json

if station_id in station_list:



'''


MADIS = r'/var/www/radiosonde.mah.priv.at/data-dev/madis'
GISC = r'/var/www/radiosonde.mah.priv.at/data-dev/gisc'
# MADIS = r'madis'
# GISC = r'gisc'
# STATION_LIST = r'station_list.json'

flights = {}


def read_br(path):
    with open(path, mode='rb') as f:
        s = f.read()
        if path.suffix == '.br':
            s = brotli.decompress(s)
        return geojson.loads(s.decode())


def walkt_tree(toplevel, directory, pattern):
    nf = 0
    nc = 0
    nu = 0
    for p in sorted(directory.rglob(pattern)):
        s = p.stem
        if s.endswith(".geojson"):
            s = s.rsplit(".", 1)[0]
        stid, day, tim = s.split('_')
        ts = ciso8601.parse_datetime(day + " " + tim + "-00:00").timestamp()
        #print(stid, day, tim, datetime.fromtimestamp(ts, pytz.utc))
        if toplevel.endswith('madis'):
            typus = "netCDF"
        if toplevel.endswith('gisc'):
            typus = "BUFR"
        entry = {
            "source": typus,
            "syn_timestamp": int(ts)
        }
        gj = None
        if stid not in station_list:
            # mobile. Use per-ascent coordinates and
            # propagate down into entry.
            gj = read_br(p)
            typus = gj.properties['id_type']
            entry['lat'] = gj.properties['lat']
            entry['lon'] = gj.properties['lon']
            entry['elevation'] = gj.properties['elevation']
        else:
            st = station_list[stid]

        if stid not in flights:
            flights[stid] = Feature(
                # FIXME add point after sorting for mobiles
                properties={
                    "ascents": [entry]
                })
        else:
            flights[stid].properties['ascents'].append(entry)

        f = flights[stid]
        f.properties['station_id'] = stid

        if stid in station_list:
            st = station_list[stid]
            f.properties['id_type'] = "wmo"
            f.properties['name'] = st['name']
            f.geometry = Point((round(st["lon"], 6),
                                round(st["lat"], 6),
                                round(st["elevation"], 1)))
        else:
            gj = read_br(p)
            # FIXME this should be from file contents
            f.properties['id_type'] = "mobile"
            f.properties['name'] = stid

        nf += 1
    return (nf, 1, 1)

    # "05467": {
    #     "name": "Sharjah",
    #     "lat": 25.25,
    #     "lon": 55.37,
    #     "elevation": 4.0
    # },
    # if stid in station_list:
    #     kind = "wmo"
    #     name = station_list[stid].name


def main(dirlist):
    global station_list
    with open(STATION_LIST) as json_file:
        station_list = json.load(json_file)
    ntotal = 0
    ttotal = 0
    for d in dirlist:
        start = time.time()
        nf, nu, nc = walkt_tree(d, pathlib.Path(d), '*.geojson.br')
        ntotal = ntotal + nf
        if nf == 0:
            continue
        end = time.time()
        dt = end - start
        ttotal += dt
        ratio = (1. - nu / nc) * 100.
        print(f"directory {d}:")
        print(
            f"{nf} files, avg uncompressed file={nc/nf:.0f},  avg compressed file={nu/nf:.0f}")
        print(
            f"avg compression={ratio:.1f}%, total time {dt:.3f}s  {dt*1000/nf:.3f}ms per file")

    print(
        f"read {ntotal} files in {ttotal:.3f}s, avg {ttotal*1000/ntotal:.3f}ms per file")
    pprint(flights)


if __name__ == "__main__":
    dirlist = [MADIS, GISC]
    #dirlist = ['gisc/', 'madis/']

    sys.exit(main(dirlist))
