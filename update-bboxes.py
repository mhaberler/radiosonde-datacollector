import argparse
import logging
import pathlib
import geojson
import time, sys, os
from multiprocessing import Pool, cpu_count
from multiprocessing_logging import install_mp_handler
from vincenty import vincenty
from pprint import pprint

import config

import pidfile

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

    util.write_json_file(fc, filename, useBrotli=True, asGeojson=True)


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

#        r = pool.starmap(write_geojson, args)

def walkt_tree(pool, directory, pattern):
    nf = 0
    for path in sorted(directory.rglob(pattern)):
        #print(path, file=sys.stderr)
        gj = util.read_json_file(path, useBrotli=True, asGeojson=True)
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
    install_mp_handler()
    os.umask(0o22)
    
    with pidfile.Pidfile(
            config.LOCKFILE + config.DATA_DIR.rstrip("/") + ".pid",
            log=logging.debug,
            warn=logging.debug,
    ) as pf, Pool(cpu_count()) as pool:
        try:
            nf = walkt_tree(pool, pathlib.Path(args.fm94),'*.geojson.br')
            
            dump_bboxes(points, args.bbox)
            logging.debug(f"{nf} flights")
            
        except pidfile.ProcessRunningException:
            logging.error(f"the pid file {config.LOCKFILE} is in use, exiting.")
            return -1

    return 0

if __name__ == "__main__":
    sys.exit(main())
