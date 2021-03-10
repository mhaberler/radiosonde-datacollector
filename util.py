from math import cos, sin, isnan, nan, pi
import json
import logging
import os
import tempfile
import time
from datetime import datetime

import numpy as np

import brotli

import config

import geojson

import constants

import customtypes

import config


def _round(val, decimals):
    return round(float(val), decimals)


def add_if_present(d, h, name, bname):
    if bname in h:
        d[name] = h[bname]


def add_if_set(d, name, value):
    if issubclass(type(value), float) and isnan(value):
        return
    if not value:
        return
    d[name] = value


# Store as JSON a numpy.ndarray or any nested-list composition.


class NumpyEncoder(json.JSONEncoder):
    """ Special json encoder for numpy types """

    def default(self, obj):
        if isinstance(obj, np.integer):
            return int(obj)
        elif isinstance(obj, np.floating):
            return float(obj)
        elif isinstance(obj, np.ndarray):
            return obj.tolist()
        return json.JSONEncoder.default(self, obj)


_mapping = {

    "station": "station_id",
    "stationName": "station_name",
    "sondTyp": "sonde_type",
    
    "encoding": None,                 
    "format":   None,                     
    "source": None,                       # "MADIS": "GISC DWD" ...
    "origin": None,
    "arrived": None,
    "text": None,

    "path_source":    None,               # simulated gps
    "relTime": "firstSeen",
    "synTime": "syn_timestamp",
    "pressureSensorType": "sonde_psensor",
    "temperatureSensorType": "sonde_tsensor",
    "humiditySensorType": "sonde_hsensor",
    "geopotentialHeightCalculation": "sonde_gepot",
    "trackingTechniqueOrStatusOfSystem": "sonde_track",
    "measuringEquipmentType": "sonde_measure",
    "softwareVersionNumber": "sonde_swversion",
    "radiosondeType": "sonde_type",
    "radiosondeSerialNumber": "sonde_serial",
    "radiosondeOperatingFrequency": "sonde_frequency",
    "correctionAlgorithmsForHumidityMeasurements": "sonde_humcorr",

    "repfmt": None,# FM-35 FM-94
    "channel": None,
    "archive": "origin_archive",
    "encoding": None,    # netCDF BUFR
    "filename": "origin_member",

}


def set_metadata(properties, position=None, **kwargs):

    assert type(properties) == customtypes.DictNoNone

    station = kwargs.get("station", None)
    wks = station and station in config.known_stations

    idType = kwargs.get("idType", None)
    properties["id_type"] = idType if idType else ("wmo" if wks else "mystery")
    properties["processed"] = now()

    position = kwargs.get("position", None)
    if position and all(not isnan(c) for c in position):
        properties["lon"] = _round(float(position[0]), 6)
        properties["lat"] = _round(float(position[1]), 6)
        properties["elevation"] = _round(float(position[2]), 1)
    elif wks:
        properties["lon"] = config.known_stations[station]["lon"]
        properties["lat"] = config.known_stations[station]["lat"]
        properties["elevation"] = config.known_stations[station]["elevation"]

    for k, v in kwargs.items():
        if k not in _mapping:
            raise KeyError(f"invalid argument: {k}")
        rk = _mapping[k]
        if isinstance(rk, str):
            properties[rk] = kwargs[k]
        if rk == None:
            properties[k] = kwargs[k]

def set_metadata_from_dict(properties, d):
    for k in _mapping:
        if k in d:
            rk = _mapping[k]
            if isinstance(rk, str):
                properties[rk] = d[k]
            if rk == None:
                properties[k] = d[k]    
            

def wind_to_UV(windSpeed, windDirection):
    if isnan(windSpeed) or isnan(windDirection):
        return nan, nan
    u = -windSpeed * sin(constants.rad * windDirection)
    v = -windSpeed * cos(constants.rad * windDirection)
    return round(u, 2), round(v, 2)


def latlonPlusDisplacement(lat=0, lon=0, u=0, v=0):
    # HeidiWare
    dLat = v / constants.mperdeg
    dLon = u / (cos((lat + dLat / 2) / 180 * pi) * constants.mperdeg)
    return lat + dLat, lon + dLon


def height_to_geopotential_height(height):
    return constants.earth_gravity / ((
        1 / height) + 1 / constants.earth_avg_radius) / constants.earth_gravity


def geopotential_height_to_height(geopotential):
    return constants.earth_gravity * (geopotential * constants.earth_avg_radius) / (constants.earth_gravity * constants.earth_avg_radius - geopotential)


def now():
    return int(datetime.utcnow().timestamp())


def age(filename):
    if not os.path.exists(filename):
        return 0
    return os.path.getmtime(filename)


def read_file(name, useBrotli=False):
    with open(name, "rb") as f:
        s = f.read()
        sl = len(s)
        if useBrotli:
            s = brotli.decompress(s)
            ratio = (sl / len(s)) * 100.0
            logging.debug(f"w {name}: brotli {sl} -> {len(s)}, {ratio:.1f}%")
        return s


def read_json_file(name, useBrotli=False, asGeojson=False):
    s = read_file(name, useBrotli=useBrotli).decode()
    if asGeojson:
        return geojson.loads(s)
    else:
        return json.loads(s)


def write_json_file(d, name, useBrotli=False, asGeojson=False):
    if asGeojson:
        b = geojson.dumps(d, indent=config.INDENT).encode(config.CHARSET)
    else:
        b = json.dumps(d, indent=config.INDENT).encode(config.CHARSET)
    write_file(b, name, useBrotli=useBrotli)


def write_file(s, name, useBrotli=False):
    fd, path = tempfile.mkstemp(dir=config.tmpdir)
    if useBrotli:
        sl = len(s)
        start = time.time()
        s = brotli.compress(s, quality=config.BROTLI_SUMMARY_QUALITY)
        end = time.time()
        dt = end - start
        dl = len(s)
        ratio = (1.0 - dl / sl) * 100.0
        logging.debug(
            f"w {name}: brotli {sl} -> {dl},"
            f" compression={ratio:.1f}% in {dt:.3f}s"
        )
    os.write(fd, s)
    os.fsync(fd)
    os.close(fd)
    os.rename(path, name)
    os.chmod(name, 0o644)
