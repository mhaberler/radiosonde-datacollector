import logging
import warnings

# warnings.filterwarnings("ignore")
# np.seterr(all='raise')
from datetime import datetime, timedelta

from math import cos, sin
from string import punctuation

import ciso8601

from constants import earth_avg_radius, earth_gravity, rad

from eccodes import (
    CODES_MISSING_DOUBLE,
    CODES_MISSING_LONG,
    codes_bufr_new_from_file,
    codes_get,
    codes_get_array,
    codes_release,
    codes_set,
)


import geojson

from config import FAKE_TIME_STEPS, MAX_FLIGHT_DURATION

class MissingKeyError(Exception):
    def __init__(self, key, message="missing required key"):
        self.key = key
        self.message = message
        super().__init__(self.message)

    def __str__(self):
        return f"{self.key} -> {self.message}"


class BufrUnreadableError(Exception):
    pass


# metpy is terminally slow, so roll our own sans dimension checking
def geopotential_height_to_height(gph):
    geopotential = gph * earth_gravity
    return (geopotential * earth_avg_radius) / (
        earth_gravity * earth_avg_radius - geopotential
    )


def bufr_decode(
    f, fn, archive, args, fakeTimes=True, fakeDisplacement=True, logFixup=True
):
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

    # special-case warts we do not really care about
    warts = ["shipOrMobileLandStationIdentifier"]

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

            invalidSamples += 1
            if not fakeTimes:
                continue
            else:
                timePeriod = fakeTimeperiod
                fakeTimeperiod += FAKE_TIME_STEPS
                if k not in fixups:
                    logging.debug(
                        f"FIXUP timePeriod fakeTimes:{fakeTimes} fakeTimeperiod={fakeTimeperiod}"
                    )
                    fixups.append(k)

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
                    if fakeDisplacement and k in replaceable:
                        if k not in fixups:
                            logging.debug(f"--FIXUP  key {k}")
                            fixups.append(k)
                        sample[k] = 0
                    else:
                        # logging.warning(f"--MISSING {i} key {k} ")
                        sampleOK = False
                        missingValues += 1
            except Exception as e:
                sampleOK = False
                logging.debug(f"sample={i} key={k} e={e}, skipping")
                missingValues += 1

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


def bufr_qc(args, h, s, fn, archive):
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


def process_bufr(args, source, f, fn, archive):
    try:
        (h, s) = bufr_decode(f, fn, archive, args)

    except Exception as e:
        logging.warning(f"exception processing {fn} e={e}")
        return False, None

    else:
        result = bufr_qc(args, h, s, fn, archive)
        h["samples"] = s
        return result, h


def wind_to_UV(windSpeed, windDirection):
    u = -windSpeed * sin(rad * windDirection)
    v = -windSpeed * cos(rad * windDirection)
    return u, v


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


def add_if_present(d, h, name, bname):
    if bname in h:
        d[name] = h[bname]


def convert_bufr_to_geojson(args, h):
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

    properties = {
        "station_id": ident,
        "id_type": typ,
        "source": "BUFR",
        "path_source": "origin",
        "syn_timestamp": ts,
        "firstSeen": takeoff.timestamp(),
        "lat": h["latitude"],
        "lon": h["longitude"],
    }
    add_if_present(properties, h, "sonde_type", "radiosondeType")
    add_if_present(properties, h, "sonde_serial", "radiosondeSerialNumber")
    add_if_present(properties, h, "sonde_frequency", "radiosondeOperatingFrequency")

    # try hard to determine a reasonable takeoff elevation value
    if "height" in h:
        properties["elevation"] = h["height"]
    elif "heightOfStationGroundAboveMeanSeaLevel" in h:
        properties["elevation"] = h["heightOfStationGroundAboveMeanSeaLevel"]
    elif "heightOfBarometerAboveMeanSeaLevel" in h:
        properties["elevation"] = h["heightOfBarometerAboveMeanSeaLevel"]
    else:
        # take height of first sample
        gph = samples[0]["nonCoordinateGeopotentialHeight"]
        properties["elevation"] = geopotential_height_to_height(gph)

    fc = geojson.FeatureCollection([])
    fc.properties = properties
    lat_t = fc.properties["lat"]
    lon_t = fc.properties["lon"]
    previous_elevation = fc.properties["elevation"] - args.hstep

    for s in samples:
        lat = lat_t + s["latitudeDisplacement"]
        lon = lon_t + s["longitudeDisplacement"]
        gpheight = s["nonCoordinateGeopotentialHeight"]

        delta = timedelta(seconds=s["timePeriod"])
        sampleTime = takeoff + delta

        height = geopotential_height_to_height(gpheight)
        if height < previous_elevation + args.hstep:
            continue
        previous_elevation = height

        u, v = wind_to_UV(s["windSpeed"], s["windDirection"])

        properties = {
            "time": sampleTime.timestamp(),
            "gpheight": gpheight,
            "temp": s["airTemperature"],
            "dewpoint": s["dewpointTemperature"],
            "pressure": s["pressure"],
            "wind_u": u,
            "wind_v": v,
        }
        f = geojson.Feature(
            geometry=geojson.Point((lon, lat, height)), properties=properties
        )
        fc.features.append(f)
    fc.properties["lastSeen"] = sampleTime.timestamp()

    duration = fc.properties["lastSeen"] - fc.properties["firstSeen"]
    if duration > MAX_FLIGHT_DURATION:
        logging.error(f"unreasonably long flight: {(duration/3600):.1f} hours")

    return fc
