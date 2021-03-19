import argparse
import logging
import pathlib
import geojson
import time, sys, os
from multiprocessing import Pool, cpu_count
from multiprocessing_logging import install_mp_handler
from vincenty import vincenty
from pprint import pprint
import numpy as np
from scipy.spatial import ConvexHull, convex_hull_plot_2d
from simplify import Simplify2D

import config

import pidfile

import util


def max_distance(lat_s, lon_s, coords):
    maxd = 0
    max_lat = None
    max_lon = None
    
    for lon, lat in coords:
        d = vincenty((lat_s, lon_s), (lat, lon), miles=False)
        if d and d > maxd:
            maxd = d
            max_lat = lat
            max_lon = lon
    return max_lat, max_lon, maxd


def detail2np(path):
    gj = util.read_json_file(path, useBrotli=True, asGeojson=True)
    if gj.properties['id_type'] != "wmo":
        return None
    coords = [x.geometry.coordinates[0:2] for x in gj.features]
    return np.array(coords) 

def walkt_tree(pool, directory, pattern, sid, hull, bbox):

    sname = station_list[sid]["name"]
    nf = 0
    p = [str(x) for x in directory.rglob(pattern)]

    r = pool.map(detail2np, p)
    n = [x for x in r if x.any()]

    points = np.concatenate(n)
    h = ConvexHull(points)

    c = [points[v].tolist()  for v in h.vertices]

    sim = Simplify2D()
    highestQuality = True
    tolerance = 0.01
    coords = sim.simplify(c,
                          tolerance=tolerance,
                          highestQuality=highestQuality,
                          returnMarkers=False)
    
    f = geojson.Polygon([coords])
    f.properties = {
        'station_id': sid,
        'name': sname
    }
    hull.features.append(f)


    llon = min(point[0] for point in c)
    llat = min(point[1] for point in c)
    rlon = max(point[0] for point in c)
    rlat = max(point[1] for point in c)
    f = geojson.Feature(geometry=geojson.MultiLineString([[(llon,llat),
                                                           (rlon,llat),
                                                           (rlon,rlat),
                                                           (llon,rlat),
                                                           (llon,llat)]]),
                        properties= {
                            'station_id': sid,
                            'name': sname
                        })
    bbox.features.append(f)
    
    slat = station_list[sid]["lat"]
    slon = station_list[sid]["lon"]
    mlat, mlon, md = max_distance(slat, slon, coords)

    f = geojson.Feature(
        geometry=geojson.Point((round(mlon, 6), round(mlat, 6))),
        properties= {
            'station_id': sid,
            'name': sname,
            'distance': md
        })
    bbox.features.append(f)
    
    return len(p)

def station_dir(repfmt, station_id, prefix=None):
    cc = station_id[:2]
    subdir = station_id[2:5]
    pn = f"{repfmt}/{cc}/{subdir}/"
    if prefix:
        return prefix + "/" +  pn
    return pn

def  main():
    parser = argparse.ArgumentParser(
        description="rebuild fm94 bounding boxes.json",
        add_help=True,
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("-v", "--verbose", action="store_true", default=False)
    parser.add_argument(
        "--area",
        action="store",
        default=config.WWW_DIR + config.DATA_DIR + config.FM94_AREA,
        help="path of brotli-compressed area-fm94.geojson.br",
    )
    parser.add_argument(
        "--bbox",
        action="store",
        default=config.WWW_DIR + config.DATA_DIR + config.FM94_BBOX,
        help="path of brotli-compressed area-fm94.geojson.br",
    )
    parser.add_argument(
        "--datadir",
        action="store",
        default=config.WWW_DIR + config.DATA_DIR,
        help="path to data dir",
    )
    parser.add_argument(
        "--station-json",
        action="store",
        default=config.STATION_LIST,
        help="path to write the station_list.json file",
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
            hull = geojson.FeatureCollection([])
            bbox = geojson.FeatureCollection([])

            global station_list
            station_list = util.read_json_file(args.station_json,
                                               useBrotli=args.station_json.endswith(".br"))
            
            for st, p in station_list.items():
                sdir =  station_dir("fm94", st, prefix=args.datadir)


                if os.path.exists(sdir):
                    name = station_list[st]["name"]
                    nf = walkt_tree(pool, pathlib.Path(sdir),'*.geojson.br', st, hull, bbox)
            util.write_json_file(hull,
                                 args.area,
                                 useBrotli=args.area.endswith(".br"),
                                 asGeojson=True)
            util.write_json_file(bbox,
                                 args.bbox,
                                 useBrotli=args.area.endswith(".br"),
                                 asGeojson=True)

            #logging.warning(f"{nf} flights")
            
        except pidfile.ProcessRunningException:
            logging.error(f"the pid file {config.LOCKFILE} is in use, exiting.")
            return -1

    return 0

if __name__ == "__main__":
    sys.exit(main())
