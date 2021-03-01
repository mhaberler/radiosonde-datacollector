import pathlib
import brotli
import geojson
import time, sys, os

import pandas as pd
from vincenty import vincenty
from pprint import pprint
import geopandas


MADIS = r'/var/www/radiosonde.mah.priv.at/data-dev/madis'
GISC = r'/var/www/radiosonde.mah.priv.at/data-dev/gisc'

def latlon(f):
    return (f['geometry']['coordinates'][1],
            f['geometry']['coordinates'][0])

#12374 [(51.71204, 20.90611), (52.81019, 23.51059)]
def dump_bboxes(points, filename):
    fc = geojson.FeatureCollection([])


    for st, pts in points.items():
        if len(pts) == 0:
            continue

        lbot,rtop = bounding_box_naive(pts)
        llon, llat = lbot
        rlon, rlat = rtop
        print(st, lbot,rtop)

        f = geojson.Feature(geometry=geojson.MultiLineString([[(llat,llon), (rlat,llon), (rlat,rlon),(llat,rlon),(llat,llon)]]),
                            properties= {
                                'station_id': st
                            })
        fc.features.append(f)
    with open(filename, "wb") as jfile:
        gj = geojson.dumps(fc, indent=4).encode("utf8")
        jfile.write(gj)


def bounding_box_naive(points):
    bot_left_x = min(point[0] for point in points)
    bot_left_y = min(point[1] for point in points)
    top_right_x = max(point[0] for point in points)
    top_right_y = max(point[1] for point in points)

    return [(bot_left_x, bot_left_y), (top_right_x, top_right_y)]

points = {}

def extent(gj):
    if gj.properties['id_type'] != "wmo":
        return
    stid = gj.properties['station_id']
    if stid not in points:
        points[stid] = []

    takeoff = latlon(gj.features[0])
    maxd = 0
    pts = []

    for f in gj.features:
        cp = latlon(f)
        d = vincenty(takeoff, cp, miles=False)
        if not d:
            continue
        pts.append(cp)
        if d > maxd:
            maxd = d

    points[stid].extend(pts)
    if d < 0.1:
        return None
    return gj.properties['station_id'], d

flights = []
def walkt_tree(directory, pattern):
    nf = 0
    nc = 0
    nu = 0
    for path in sorted(directory.rglob(pattern)):
        #print(path, file=sys.stderr)
        with open(path, mode='rb') as f:
            s = f.read()
            nu += len(s)
            if path.suffix == '.br':
                s = brotli.decompress(s)
                nc += len(s)
            gj = geojson.loads(s.decode())
            nf += 1
            r = extent(gj)
            if r:
                flights.append(r)
    return nf

def  main(dirlist):
    nf = 0
    for d in dirlist:
        nf += walkt_tree(pathlib.Path(d),'*.geojson.br')

    dump_bboxes(points, "flight-bbox.geojson")


    f = sorted(flights, key=lambda tup: tup[1])
    print(f"{nf} flights")
    pprint(f)

if __name__ == "__main__":
    dirlist = [GISC]
    #dirlist = ['gisc/']

    sys.exit(main(dirlist))
