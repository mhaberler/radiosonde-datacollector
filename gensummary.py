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
from config import STATION_LIST, MAX_ASCENT_AGE_IN_SUMMARY, FORMAT_VERSION
from geojson import Feature, FeatureCollection, Point
from pprint import pprint
import brotli
import re
from operator import itemgetter
import reverse_geocoder as rg

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
MADIS = r'madis'
GISC = r'gisc'
STATION_LIST = r'station_list.json'

flights = {}
missing = {}
txtfrag = []

def now():
    return datetime.utcnow().timestamp()

def read_br(path):
    with open(path, mode='rb') as f:
        s = f.read()
        if path.suffix == '.br':
            s = brotli.decompress(s)
        return geojson.loads(s.decode())


def walkt_tree(toplevel, directory, pattern, after):
    nf = 0
    nc = 0
    nu = 0
    for p in sorted(directory.rglob(pattern)):
        s = p.stem
        if s.endswith(".geojson"):
            s = s.rsplit(".", 1)[0]
        stid, day, tim = s.split('_')
        ts = ciso8601.parse_datetime(day + " " + tim + "-00:00").timestamp()

        if ts < after:
            #print("skipping", s, file=sys.stderr)
            continue

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
            # maybe mobile. Check ascent for type
            # example unregistered, but obviously fixed:
            # https://radiosonde.mah.priv.at/data-dev/gisc/08/383/08383_20210206_120000.geojson
            # check id syntax - 5 digits = unregistered else mobile
            if re.match(r"^\d{5}$",stid):
                # WMO id syntax, but not in station_list
                # hence an unregistered but fixed station
                idtype = 'unregistered'
            else:
                # could be ship registration syntax. Check detail file.
                gj = read_br(p)
                idtype = gj.properties['id_type']
                # propagate per-ascent coords down to ascent
                entry['lat'] = round(gj.properties['lat'], 6)
                entry['lon'] = round(gj.properties['lon'], 6)
                entry['elevation'] = round(gj.properties['elevation'],2)
        else:
            # registered
            st = station_list[stid]
            idtype = 'wmo'

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
        f.properties['id_type'] = idtype
        f.properties['name'] = stid

        if stid in station_list:
            st = station_list[stid]
            # override name if we have one
            f.properties['name'] = st['name']
        else:
            if idtype == 'unregistered':
                if stid not in missing:
                    locations = rg.search((st["lat"], st["lon"]))
                    if locations:
                        print(stid, locations, st["lat"], st["lon"], st["elevation"],file=sys.stderr)
                        loc = locations[0]
                        f.properties['name'] = loc['name'] + ", " + loc['cc']
                        s = f'{stid.rjust(11, "X")} {st["lat"]} {st["lon"]} {st["elevation"]} {f.properties["name"]} 2020'
                        txtfrag.append(s)
                    missing[stid] = {
                        "name" : f.properties['name'],
                        "lat" :  st["lat"],
                        "lon" :  st["lon"],
                        "elevation" :  st["elevation"]
                    }
        # this needs fixing up for mobiles after sorting
        f.geometry = Point((round(st["lon"], 6),
                            round(st["lat"], 6),
                            round(st["elevation"], 1)))
        nf += 1
    return (nf, 1, 1)

def fixup_flights(flights):
    # pass 1: reverse sort ascents by timestamp
    for stid, f in flights.items():
        a = f.properties['ascents']
        f.properties['ascents'] = sorted(a,
                                         key=itemgetter("syn_timestamp"),
                                         reverse=True)

    # pass 2: for mobile stations, propagate up
    # coords of newest ascent to geometry.coords
    for stid, f in flights.items():
        if f.properties['id_type'] == 'mobile':
            latest = f.properties['ascents'][0]
            f.geometry = Point((round(latest["lon"], 6),
                                round(latest["lat"], 6),
                                round(latest["elevation"], 1)))

def gen_br_file(gj, tmpdir, dest):
    if not dest.endswith(".geojson"):
        dest += ".geojson"
    if not dest.endswith(".br"):
        dest += ".br"
    fd, path = tempfile.mkstemp(dir=tmpdir)
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

def main(dirlist):
    cutoff_ts = now() - MAX_ASCENT_AGE_IN_SUMMARY
    global station_list
    with open(STATION_LIST) as json_file:
        station_list = json.load(json_file)
    ntotal = 0
    ttotal = 0
    for d in dirlist:
        start = time.time()
        nf, nu, nc = walkt_tree(d, pathlib.Path(d), '*.geojson.br', cutoff_ts)
        ntotal = ntotal + nf

    fixup_flights(flights)
    fc = geojson.FeatureCollection([])
    fc.properties = {
        "fmt":  FORMAT_VERSION,
        "generated": int(now())
    }
    for _st, f in flights.items():
        fc.features.append(f)
    print(geojson.dumps(fc, indent=4))
    #print(json.dumps(missing, indent=4), file=sys.stderr)
    for l in txtfrag:
        print(l, file=sys.stderr)

if __name__ == "__main__":
    dirlist = [MADIS, GISC]
    #dirlist = ['gisc/', 'madis/']

    sys.exit(main(dirlist))
