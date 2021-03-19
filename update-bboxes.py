import argparse
import pathlib
import brotli
import geojson
import time, sys, os

import pandas as pd
from vincenty import vincenty
from pprint import pprint
import geopandas

import config

import util

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
        logging.debug(f"w {st}: {lbot} {rtop}")

        f = geojson.Feature(geometry=geojson.MultiLineString([[(llat,llon), (rlat,llon), (rlat,rlon),(llat,rlon),(llat,llon)]]),
                            properties= {
                                'station_id': st
                            })
        fc.features.append(f)

    write_json_file(fc, filename, useBrotli=True, asGeojson=True)


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
        gj = util.read_json_file(path, useBrotli=True, asGeojson=True):
        nf += 1
        r = extent(gj)
        if r:
            flights.append(r)
    return nf

def  main():
    parser = argparse.ArgumentParser(
        description="rebuild fm94 bounding boxes.json",
        add_help=True,
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("-v", "--verbose", action="store_true", default=False)
    parser.add_argument(
        "--bbox",
        action="store",
        default=config.WWW_DIR + config.DATA_DIR + config.FM94_BBOX,
        help="path of brotli-compressed bbox-fm94.geojson.br",
    )
    parser.add_argument(
        "--fm94",
        action="store",
        default=config.FM94_DATA,
        help="path to fm94 dir",
    )
    args = parser.parse_args()

    level = logging.WARNING
    if args.verbose:
        level = logging.DEBUG

    logging.basicConfig(level=level)
    os.umask(0o22)

    nf = walkt_tree(pathlib.Path(args.fm94),'*.geojson.br')
    dump_bboxes(points, args.bbox)

    logging.debug(f"{nf} flights")


if __name__ == "__main__":
    sys.exit(main(dirlist))
