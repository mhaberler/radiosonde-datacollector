import logging
from datetime import datetime, timedelta

from string import punctuation

import ciso8601

import config

import util

from eccodes import (
    CODES_MISSING_DOUBLE,
    CODES_MISSING_LONG,
    codes_bufr_new_from_file,
    codes_get,
    codes_get_array,
    codes_release,
    codes_set,
)

import customtypes

import geojson


class MissingKeyError(Exception):
    def __init__(self, key, message="missing required key"):
        self.key = key
        self.message = message
        super().__init__(self.message)

    def __str__(self):
        return f"{self.key} -> {self.message}"


class BufrUnreadableError(Exception):
    pass


def bufr_decode(f, filename):
    
    ibufr = codes_bufr_new_from_file(f)
    if not ibufr:
        raise BufrUnreadableError("empty file", fn, archive)
    codes_set(ibufr, "unpack", 1)

    missingHdrKeys = 0
    header = {}
    try:
        k = "extendedDelayedDescriptorReplicationFactor"
        num_samples = codes_get_array(ibufr, k)[0]
    except Exception as e:
        codes_release(ibufr)
        raise MissingKeyError(k, message=f"cant determine number of samples: {e}")

    # BAIL HERE if no num_samples

    ivals = [
        "typicalYear",
        "typicalMonth",
        "typicalDay",
        "typicalHour",
        "typicalMinute",
        "typicalSecond",
        "blockNumber",
        "stationNumber",
        "radiosondeType",
        "height",
        "year",
        "month",
        "day",
        "hour",
        "minute",
        "second",
        "correctionAlgorithmsForHumidityMeasurements",
        "pressureSensorType",
        "temperatureSensorType",
        "humiditySensorType",
        "geopotentialHeightCalculation",
        "trackingTechniqueOrStatusOfSystem",
        "measuringEquipmentType",
    ]
    fvals = [
        "radiosondeOperatingFrequency",
        "latitude",
        "longitude",
        "heightOfStationGroundAboveMeanSeaLevel",
        "heightOfBarometerAboveMeanSeaLevel",
    ]
    svals = [
        "radiosondeSerialNumber",
        "typicalDate",
        "typicalTime",
        "softwareVersionNumber",
    ]

    for k in ivals + fvals + svals:
        try:
            value = codes_get(ibufr, k)
            if k in ivals:
                if value != CODES_MISSING_LONG:
                    header[k] = value
            elif k in fvals:
                if value != CODES_MISSING_DOUBLE:
                    header[k] = value
            elif k in svals:
                header[k] = value
            else:
                pass
        except Exception as e:
            logging.debug(f"missing header key={k} e={e}")
            missingHdrKeys += 1

    # special-case warts we do not really care about so do not log
    warts = ["shipOrMobileLandStationIdentifier", "text"]

    for k in warts:
        try:
            header[k] = codes_get(ibufr, k)
        except Exception:
            missingHdrKeys += 1

    fkeys = [  # 'extendedVerticalSoundingSignificance',
        "pressure",
        "nonCoordinateGeopotentialHeight",
        "latitudeDisplacement",
        "longitudeDisplacement",
        "airTemperature",
        "dewpointTemperature",
        "windDirection",
        "windSpeed",
    ]

    samples = []
    invalidSamples = 0
    missingValues = 0
    fakeTimeperiod = 0
    fixups = []  # report once only

    for i in range(1, num_samples + 1):
        sample = {}

        k = "timePeriod"
        timePeriod = codes_get(ibufr, f"#{i}#{k}")
        if timePeriod == CODES_MISSING_LONG:
            continue
        
            # invalidSamples += 1
            # if not fakeTimes:
            #     continue
            # else:
            #     timePeriod = fakeTimeperiod
            #     fakeTimeperiod += config.FAKE_TIME_STEPS
            #     if k not in fixups:
            #         logging.debug(
            #             f"FIXUP timePeriod fakeTimes:{fakeTimes} fakeTimeperiod={fakeTimeperiod}"
            #         )
            #         fixups.append(k)

        sample[k] = timePeriod
        replaceable = ["latitudeDisplacement", "longitudeDisplacement"]
        sampleOK = True
        for k in fkeys:
            name = f"#{i}#{k}"
            try:
                value = codes_get(ibufr, name)
                if value != CODES_MISSING_DOUBLE:
                    sample[k] = value
                else:
                    sampleOK = False
                    missingValues += 1
                    break
                    
                    # if fakeDisplacement and k in replaceable:
                    #     if k not in fixups:
                    #         logging.debug(f"--FIXUP  key {k}")
                    #         fixups.append(k)
                    #     sample[k] = 0
                    # else:
                    #     # logging.warning(f"--MISSING {i} key {k} ")
                    #     sampleOK = False
                    #     missingValues += 1

                        
            except Exception as e:
                sampleOK = False
                logging.debug(f"sample={i} key={k} e={e}, skipping")
                missingValues += 1
                break

        if sampleOK:
            samples.append(sample)

    logging.debug(
        (
            f"samples used={len(samples)}, invalid samples="
            f"{invalidSamples}, skipped header keys={missingHdrKeys},"
            f" missing values={missingValues}"
        )
    )

    codes_release(ibufr)
    return header, samples


def bufr_qc(h, s, fn, archive):
    if len(s) < 10:
        logging.info(f"QC: skipping {fn} from {archive} - only {len(s)} samples")
        return False

    # QC here!
    if not ({"year", "month", "day", "minute", "hour"} <= h.keys()):
        logging.info(f"QC: skipping {fn} from {archive} - day/time missing")
        return False

    if "second" not in h:
        h["second"] = 0  # dont care
    return True


def process_bufr(f, filename=None, archive=None):
    try:
        (h, s) = bufr_decode(f, filename)

    except Exception as e:
        logging.warning(f"exception processing {filename} e={e}")
        return None

    else:
        if not bufr_qc(h, s, filename, archive):
            return None
        h["samples"] = s
        return h


def gen_id(h):
    bn = h.get("blockNumber", CODES_MISSING_LONG)
    sn = h.get("stationNumber", CODES_MISSING_LONG)

    if bn != CODES_MISSING_LONG and sn != CODES_MISSING_LONG:
        return ("wmo", f"{bn:02d}{sn:03d}")

    if "shipOrMobileLandStationIdentifier" in h:
        ident = h["shipOrMobileLandStationIdentifier"]
        # if it looks remotely like an id...
        if not any(p in ident for p in punctuation):
            return ("mobile", h["shipOrMobileLandStationIdentifier"])

    return ("location", f"{h['latitude']:.3f}:{h['longitude']:.3f}")


def convert_bufr_to_geojson(h,
                            filename=None,
                            archive=None,
                            arrived=None,
                            channel=None):
    takeoff = datetime(
        year=h["year"],
        month=h["month"],
        day=h["day"],
        minute=h["minute"],
        hour=h["hour"],
        second=h["second"],
        tzinfo=None,
    )

    samples = h["samples"]
    typ, ident = gen_id(h)

    ts = ciso8601.parse_datetime(
        h["typicalDate"] + " " + h["typicalTime"] + "-00:00"
    ).timestamp()

    # try hard to determine a reasonable takeoff elevation value
    if "height" in h:
        ele = h["height"]
    elif "heightOfStationGroundAboveMeanSeaLevel" in h:
        ele = h["heightOfStationGroundAboveMeanSeaLevel"]
    elif "heightOfBarometerAboveMeanSeaLevel" in h:
        ele = h["heightOfBarometerAboveMeanSeaLevel"]
    else:
        # take height of first sample
        gph = samples[0]["nonCoordinateGeopotentialHeight"]
        ele = round(util.geopotential_height_to_height(gph), 2)
  
    properties = customtypes.DictNoNone()
    util.set_metadata(
        properties,
        station=ident,
        id_type=typ,
        # stationName=station_name,
        position=(h["longitude"], h["latitude"], ele),
        filename=filename,
        archive=archive,
        arrived=arrived,
        synTime=int(int(ts)),
        relTime=int(takeoff.timestamp()),
        # origin=origin,
        repfmt="fm94",
        path_source="origin",
        encoding="BUFR",
        channel=channel,
    )

    util.set_metadata_from_dict(properties, h)

    fc = geojson.FeatureCollection([])
    fc.properties = properties
    lat_t = fc.properties["lat"]
    lon_t = fc.properties["lon"]
    previous_elevation = fc.properties["elevation"] - config.HSTEP

    for s in samples:
        lat = lat_t + s["latitudeDisplacement"]
        lon = lon_t + s["longitudeDisplacement"]
        gpheight = s["nonCoordinateGeopotentialHeight"]

        delta = timedelta(seconds=s["timePeriod"])
        sampleTime = takeoff + delta

        height = util.geopotential_height_to_height(gpheight)
        if height < (previous_elevation + config.HSTEP):
            continue
        previous_elevation = height

        u, v = util.wind_to_UV(s["windSpeed"], s["windDirection"])

        properties = customtypes.DictNoNone(
            init={
                "time": int(sampleTime.timestamp()),
                "gpheight": round(gpheight, 2),
                "temp": round(s["airTemperature"], 2),
                "dewpoint": round(s["dewpointTemperature"], 2),
                "pressure": round(s["pressure"] / 100.0, 2),
                "wind_u": round(u, 2),
                "wind_v": round(v, 2),
            }
        )
        f = geojson.Feature(
            geometry=geojson.Point((round(lon, 6), round(lat, 6), round(height, 2))),
            properties=properties,
        )
        fc.features.append(f)
    fc.properties["lastSeen"] = int(sampleTime.timestamp())

    duration = fc.properties["lastSeen"] - fc.properties["firstSeen"]
    if duration > config.MAX_FLIGHT_DURATION:
        logging.error(f"unreasonably long flight: {(duration/3600):.1f} hours")

    return fc


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
    parser.add_argument("-g", "--geojson", action="store_true", default=False)
    parser.add_argument(
        "--station-json",
        action="store",
        default='station_list.json',
        help="path to known list of stations (JSON)",
    )
    parser.add_argument("files", nargs="*")
    args = parser.parse_args()

    level = logging.WARNING
    if args.verbose:
        level = logging.DEBUG
    logging.basicConfig(level=level)

    config.known_stations = json.loads(util.read_file(args.station_json).decode())

    for filename in args.files:
        with open(filename, "rb") as f:

            result = process_bufr(f, filename=filename, archive=None)

            if args.json:
                print(json.dumps(result, indent=4, cls=util.NumpyEncoder))
            
            if args.geojson:
                arrived = int(util.age(filename))
                gj = convert_bufr_to_geojson(result, arrived=arrived)
                print(json.dumps(gj, indent=4, cls=util.NumpyEncoder))
