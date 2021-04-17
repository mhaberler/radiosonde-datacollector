import logging
from math import cos, isinf, isnan, pi
from operator import itemgetter
from pprint import pprint

import util as u

import config

import geojson

from netCDF4 import Dataset

import numpy as np

import thermodynamics as td

import customtypes

import warnings

warnings.filterwarnings('error')

def height2time(h0, height):
    hdiff = height - h0
    return hdiff / config.ASCENT_RATE


def process_netcdf(data,
                   station_name=None,
                   origin=None,
                   filename=None,
                   arrived=None,
                   archive=None,
                   pathSource=None,
                   source=None):


# tag samples with the section source of an FM35 report:
# mandatory, sig temp, sig wind, max wind
# aids in debugging strange values
# "mandatory", "sig_temp", "sig_wind", "max_wind"
#TAG_FM35 = True


    try:
        nc = Dataset("inmemory.nc", memory=data)
    except Exception as e:
        logging.error(f"exception {e} reading {f} as netCDF")
        return False, None

    recNum = nc.dimensions["recNum"].size
    manLevel = nc.dimensions["manLevel"].size
    sigTLevel = nc.dimensions["sigTLevel"].size
    sigWLevel = nc.dimensions["sigWLevel"].size
    sigPresWLevel = nc.dimensions["sigPresWLevel"].size

    wmo_ids = nc.variables["wmoStaNum"][:].filled(fill_value=np.nan)
    results = []

    for i, stn in enumerate([str(ident).zfill(5) for ident in wmo_ids]):
        if station_name and station_name != stn:
            continue

        # per station properties
        sondTyp = nc.variables["sondTyp"][i].filled(fill_value=np.nan)
        staLat = nc.variables["staLat"][i].filled(fill_value=np.nan)
        staLon = nc.variables["staLon"][i].filled(fill_value=np.nan)
        staElev = nc.variables["staElev"][i].filled(fill_value=np.nan)
        synTime = nc.variables["synTime"][i].filled(fill_value=np.nan)
        relTime = nc.variables["relTime"][i].filled(fill_value=np.nan)


        properties = customtypes.DictNoNone()
        u.set_metadata(properties,
                       station=stn,
                       stationName=station_name,
                       position=(staLon, staLat, staElev),
                       filename=filename,
                       archive=archive,
                       arrived=arrived,
                       synTime=int(synTime),
                       relTime=int(relTime),
                       sondTyp=int(sondTyp),
                       origin=origin,
                       path_source=pathSource,
                       fmt=config.FORMAT_VERSION)

        # pressure, temp, geopot height, spread at mandatory levels
        prMan = nc.variables["prMan"][i].filled(fill_value=np.nan)
        tpMan = nc.variables["tpMan"][i].filled(fill_value=np.nan)
        htMan = nc.variables["htMan"][i].filled(fill_value=np.nan)

        # "dew point depression" = "spread" for the rest of us
        tdMan = nc.variables["tdMan"][i].filled(fill_value=np.nan)

        # wind at mandatory levels
        wsMan = nc.variables["wsMan"][i].filled(fill_value=np.nan)
        wdMan = nc.variables["wdMan"][i].filled(fill_value=np.nan)

        temp = []
        # mandatory levels must have all of p d t gph, optionally speed dir
        for j in range(manLevel):
            if isnan(prMan[j]) or isnan(tpMan[j]) or isnan(htMan[j]) or isnan(tdMan[j]):
                continue

            # ws,wd nan is ok

            # yes, negative heights found in the wild:
            if htMan[j] < 0:
                continue
            wu, wv = u.wind_to_UV(wsMan[j], wdMan[j])
            h = u.geopotential_height_to_height(htMan[j])

            f = config.CUSTOM_TEMP_POINT_MASK_MANDATORY_LEVEL
            if j == 0:
                f |= config.TEMP_POINT_MASK_SURFACE

            sample = customtypes.DictNoNone(init={
                "pressure": u._round(prMan[j], 2),
                "dewpoint": u._round(tpMan[j] - tdMan[j], 2),
                "temp": u._round(tpMan[j], 2),
                "gpheight": u._round(htMan[j], 1),
                "height": u._round(h, 1),
                "wind_u": u._round(wu, 2),
                "wind_v": u._round(wv, 2),
                "flags" : f
            })
            temp.append(sample)

        # pick a base level which has valid p, t, and gph
        refLevel = -1
        mustHave = ("pressure", "temp", "gpheight","height")
        for l in range(len(temp)):
            o = temp[l]
            if all(k in o for k in mustHave) and all(not isnan(o[k]) for k in mustHave):
                p0 = o["pressure"]
                t0 = o["temp"]
                h0 = o["height"]
                refLevel = l
                break

        if refLevel < 0:
            #pprint(temp)
            logging.debug(f"skipping station {stn} - no ref level with press, gpheight and temp found, obs={len(temp)}")
            continue

        # sig Temp levels have pressure, temp, spread
        prSigT = nc.variables["prSigT"][i].filled(fill_value=np.nan)
        tpSigT = nc.variables["tpSigT"][i].filled(fill_value=np.nan)
        tdSigT = nc.variables["tdSigT"][i].filled(fill_value=np.nan)

        # pressure dewpoint temp at sig T levels
        for j in range(sigTLevel):
            if isnan(prSigT[j]) or isnan(tpSigT[j]) or isnan(tdSigT[j]):
                continue

            # add gph, h
            h = round(td.barometric_equation_inv(h0, t0, p0, prSigT[j]), 1)
            gph = u.height_to_geopotential_height(h)
            sample = customtypes.DictNoNone(init={
                "pressure": u._round(prSigT[j], 2),
                "dewpoint": u._round(tpSigT[j] - tdSigT[j], 2),
                "temp": u._round(tpSigT[j], 2),
                "gpheight": u._round(gph, 1),
                "height": u._round(h, 1),
                "flags": config.TEMP_POINT_MASK_SIGNIFICANT_TEMPERATURE_LEVEL,
            })
            temp.append(sample)

        # sig Wind levels:
        # should have pressure, geopot height, speed, direction
        # the following would be nice to have, but I found them all to
        # be missing (nan) so fill in via height and barometric equation:
        #prSigW = nc.variables["prSigW"][i].filled(fill_value=np.nan)

        # Geopotential - Significant level wrt W
        htSigW = nc.variables["htSigW"][i].filled(fill_value=np.nan)
        # "Wind Speed - Significant level wrt W" ;
        wsSigW = nc.variables["wsSigW"][i].filled(fill_value=np.nan)
        # "Wind Direction - Significant level wrt W"
        wdSigW = nc.variables["wdSigW"][i].filled(fill_value=np.nan)

        # sig wind levels p gph u v
        try:
            for j in range(sigWLevel):
                if isnan(htSigW[j]) or isnan(wsSigW[j]) or isnan(wdSigW[j]):
                    continue

                # add height, derive pressure
                h = u.geopotential_height_to_height(htSigW[j])
                p = td.barometric_equation(p0, t0, h - h0)
                wu, wv = u.wind_to_UV(wsSigW[j], wdSigW[j])

                sample = customtypes.DictNoNone()
                sample.update({
                    "pressure": u._round(p, 2),
                    "gpheight": u._round(htSigW[j], 1),
                    "height": u._round(h, 1),
                    "wind_u": u._round(wu, 2),
                    "wind_v": u._round(wv, 2),
                    "flags": config.TEMP_POINT_MASK_SIGNIFICANT_WIND_LEVEL
                })
                temp.append(sample)
        except RuntimeWarning as e:
            logging.exception(f"Exception: station {stn} {filename=} : {e}")
            continue


        # maximum wind levels
        prMaxW = nc.variables["prMaxW"][i].filled(fill_value=np.nan) # pressure@maxwind n
        wdMaxW = nc.variables["wdMaxW"][i].filled(fill_value=np.nan) # winddir@maxwind n
        wsMaxW = nc.variables["wsMaxW"][i].filled(fill_value=np.nan) # windspeed@maxwind n

        numMwnd = int(nc.variables["numMwnd"][i]) # number of max wind levels

        for j in range(numMwnd):
            if isnan(prMaxW[j]) or isnan(wsMaxW[j]) or isnan(wdMaxW[j]):
                continue

            logging.debug(f"station {stn}: maxwind[{j}] {prMaxW[j]=} {wdMaxW[j]=} {wsMaxW[j]=}")

            # add gph, h
            h = round(td.barometric_equation_inv(h0, t0, p0, prMaxW[j]), 1)
            gph = u.height_to_geopotential_height(h)
            wu, wv = u.wind_to_UV(wsMaxW[j], wdMaxW[j])

            sample = customtypes.DictNoNone(init={
                "pressure": u._round(prMaxW[j], 2),
                "gpheight": u._round(gph, 1),
                "height": u._round(h, 1),
                "wind_u": u._round(wu, 2),
                "wind_v": u._round(wv, 2),
                "flags": config.TEMP_POINT_MASK_MAXIMUM_WIND_LEVEL,
            })
            temp.append(sample)


        # tropopause levels
        prTrop = nc.variables["prTrop"][i].filled(fill_value=np.nan) # pressure@tropopause n
        tpTrop = nc.variables["tpTrop"][i].filled(fill_value=np.nan) # temp@tropopause n
        tdTrop = nc.variables["tdTrop"][i].filled(fill_value=np.nan) # spread@tropopause n
        wdTrop = nc.variables["wdTrop"][i].filled(fill_value=np.nan) # winddir@tropopause n
        wsTrop = nc.variables["wsTrop"][i].filled(fill_value=np.nan) # windspeed@tropopause n

        numTrop = int(nc.variables["numTrop"][i]) # number of tropopause levels

        for j in range(numTrop):
            if isnan(prTrop[j]):
                continue

            logging.debug(f"station {stn}: tropopause[{j}] {prTrop[j]=} {tpTrop[j]=} {tdTrop[j]=} {wdTrop[j]=} {wsTrop[j]=}")

            # add gph, h
            h = round(td.barometric_equation_inv(h0, t0, p0, prTrop[j]), 1)
            gph = u.height_to_geopotential_height(h)
            wu, wv = u.wind_to_UV(wsTrop[j], wdTrop[j])

            sample = customtypes.DictNoNone(init={
                "pressure": u._round(prTrop[j], 2),
                "gpheight": u._round(gph, 1),
                "height": u._round(h, 1),
                "dewpoint": u._round(tpTrop[j] - tdTrop[j], 2),
                "temp": u._round(tpTrop[j], 2),
                "wind_u": u._round(wu, 2),
                "wind_v": u._round(wv, 2),
                "flags": config.TEMP_POINT_MASK_TROPOPAUSE_LEVEL
            })
            temp.append(sample)


        # sort descending by pressure
        obs = sorted([{key: value for (key, value)
                       in d.items()} for d in temp],
                     key=itemgetter('pressure'), reverse=True)

        if len(obs) == 0:
            logging.debug(f"skipping station {stn} - no valid observations, fn={filename})")
            continue

        if config.GENERATE_PATHS:
            takeoff = relTime
            prevSecsIntoFlight = 0

        lat_t = properties["lat"]
        lon_t = properties["lon"]
        fc = geojson.FeatureCollection([])
        fc.properties = properties

        for o in obs:
            if config.GENERATE_PATHS:
                # gross haque to determine rough time of sample
                secsIntoFlight = height2time(h0, height)
                sampleTime = takeoff + secsIntoFlight
                o["time"] = int(sampleTime)

                wu = o.get("wind_u", None)
                wv = o.get("wind_v", None)
                if wu and wv:
                    dt = secsIntoFlight - prevSecsIntoFlight
                    du = wu * dt
                    dv = wv * dt
                    lat_t, lon_t = u.latlonPlusDisplacement(
                        lat=lat_t, lon=lon_t, u=du, v=dv)
                    prevSecsIntoFlight = secsIntoFlight

            height = o["height"]
            # it is in geometry.coordinates[2] anyway, so delete
            del o["height"]

            f = geojson.Feature(
                geometry=geojson.Point((u._round(lon_t, 6),
                                        u._round(lat_t, 6),
                                        u._round(height, 1))),
                properties=o)
            fc.features.append(f)
        if fc:
            results.append(fc)
    return results


if __name__ == "__main__":
    import argparse
    import json
    parser = argparse.ArgumentParser(
        description="extract ascent from netCDF file",
        add_help=True,
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("-v", "--verbose", action="store_true", default=False)
    parser.add_argument("-j", "--json", action="store_true", default=False)
    parser.add_argument(
        "--station-json",
        action="store",
        default='station_list.json',
        help="path to known list of stations (JSON)",
    )
    parser.add_argument(
        "-s", "--station",
        action="store",
        default=None,
        help="station name to extract"
    )
    parser.add_argument("files", nargs="*")
    args = parser.parse_args()

    level = logging.WARNING
    if args.verbose:
        level = logging.DEBUG
    logging.basicConfig(level=level,
                        format='%(levelname)-3.3s:%(module)s:%(funcName)s:%(lineno)d  %(message)s')

    config.known_stations = json.loads(u.read_file(args.station_json).decode())

    for filename in args.files:
        with open(filename, "rb") as f:
            arrived = int(u.age(filename))
            data = f.read()
            results = process_netcdf(data,
                                    station_name=args.station,
                                    origin=None,
                                    filename=None,
                                    arrived=arrived,
                                    archive=None)
        if args.json and len(results) > 0:
            print(json.dumps(results, indent=4, cls=u.NumpyEncoder))
