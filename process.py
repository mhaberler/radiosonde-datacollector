
from pathlib import Path, PurePath
from thermodynamics import barometric_equation_inv
from string import punctuation
from scipy.interpolate import interp1d
from pprint import pprint
from operator import itemgetter
from netCDF4 import Dataset
from math import cos, pi, isnan, isinf, atan, sin, cos
from eccodes import *
from datetime import datetime, timezone, timedelta, date
import re
import csv
import zipfile
import traceback
import tempfile
import sys
import pytz
import os
import orjson
import numpy as np
import logging
import json
import gzip
import geojson
import ciso8601
import brotli
import argparse
import time
import warnings
#warnings.filterwarnings("ignore")

#np.seterr(all='raise')
# lifted from https://github.com/tjlang/SkewT


earth_avg_radius = 6371008.7714
earth_gravity = 9.80665
mperdeg = 111320.0
rad = 4.0 * atan(1) / 180.

ASCENT_RATE = 5  # m/s = 300m/min

MAX_FLIGHT_DURATION = 3600 * 5  # rather unlikely
FAKE_TIME_STEPS = 30  # assume 30sec update interval
BROTLI_SUMMARY_QUALITY = 11 #7

# drop ascents older than MAX_ASCENT_AGE_IN_SUMMARY from summary
# the files are kept nevertheless
MAX_ASCENT_AGE_IN_SUMMARY = 3 * 3600 *24

# metpy is terminally slow, so roll our own sans dimension checking
def geopotential_height_to_height(gph):
    geopotential = gph * earth_gravity
    return (geopotential * earth_avg_radius) / (earth_gravity * earth_avg_radius - geopotential)


class OneLineExceptionFormatter(logging.Formatter):
    def formatException(self, exc_info):
        result = super(OneLineExceptionFormatter,
                       self).formatException(exc_info)
        return repr(result)  # or format into one line however you want to

    def format(self, record):
        s = super(OneLineExceptionFormatter, self).format(record)
        if record.exc_text:
            s = s.replace('\n', '') + '|'
        return s


class MissingKeyError(Exception):
    def __init__(self, key, message="missing required key"):
        self.key = key
        self.message = message
        super().__init__(self.message)

    def __str__(self):
        return f'{self.key} -> {self.message}'

class BufrUnreadableError(Exception):
    pass


def bufr_decode(f, fn, archive, args, fakeTimes=True, fakeDisplacement=True, logFixup=True):

    ibufr = codes_bufr_new_from_file(f)
    if not ibufr:
        raise BufrUnreadableError("empty file", fn, archive)
    codes_set(ibufr, 'unpack', 1)

    missingHdrKeys = 0
    header = dict()
    try:
        k = 'extendedDelayedDescriptorReplicationFactor'
        num_samples = codes_get_array(ibufr, k)[0]
    except Exception as e:
        codes_release(ibufr)
        raise MissingKeyError(k, message="cant determine number of samples")

    # BAIL HERE if no num_samples

    ivals = [
        'typicalYear',
        'typicalMonth',
        'typicalDay',
        'typicalHour',
        'typicalMinute',
        'typicalSecond',
        'blockNumber',
        'stationNumber',
        'radiosondeType',
        'height',
        'year',
        'month',
        'day',
        'hour',
        'minute',
        'second',
    ]
    fvals = [
        'radiosondeOperatingFrequency',
        'latitude',
        'longitude',
        'heightOfStationGroundAboveMeanSeaLevel',
        'heightOfBarometerAboveMeanSeaLevel',
    ]
    svals = [
        'radiosondeSerialNumber',
        'typicalDate',
        'typicalTime',
    ]

    for k in ivals + fvals + svals:
        try:
            value = codes_get(ibufr, k)
            if k in ivals:
                if value != eccodes.CODES_MISSING_LONG:
                    header[k] = value
            elif k in fvals:
                if value != eccodes.CODES_MISSING_DOUBLE:
                    header[k] = value
            elif k in svals:
                header[k] = value
            else:
                pass
        except Exception as e:
            logging.debug(f"missing header key={k} e={e}")
            missingHdrKeys += 1

    # special-case warts we do not really care about
    warts = ['shipOrMobileLandStationIdentifier'
             ]

    for k in warts:
        try:
            header[k] = codes_get(ibufr, k)
        except Exception:
            missingHdrKeys += 1

    fkeys = [  # 'extendedVerticalSoundingSignificance',
        'pressure',
        'nonCoordinateGeopotentialHeight',
        'latitudeDisplacement',
        'longitudeDisplacement',
        'airTemperature',
        'dewpointTemperature',
        'windDirection',
        'windSpeed']

    samples = []
    invalidSamples = 0
    missingValues = 0
    fakeTimeperiod = 0
    fixups = []  # report once only

    for i in range(1, num_samples + 1):
        sample = dict()

        k = 'timePeriod'
        timePeriod = codes_get(ibufr, f"#{i}#{k}")
        if timePeriod == eccodes.CODES_MISSING_LONG:

            invalidSamples += 1
            if not fakeTimes:
                continue
            else:
                timePeriod = fakeTimeperiod
                fakeTimeperiod += FAKE_TIME_STEPS
                if not k in fixups:
                    logging.debug(
                        f"--FIXUP timePeriod fakeTimes:{fakeTimes} fakeTimeperiod={fakeTimeperiod}")
                    fixups.append(k)

        sample[k] = timePeriod

        replaceable = ['latitudeDisplacement', 'longitudeDisplacement']
        sampleOK = True
        for k in fkeys:
            name = f"#{i}#{k}"
            try:
                value = codes_get(ibufr, name)
                if value != eccodes.CODES_MISSING_DOUBLE:
                    sample[k] = value
                else:
                    if fakeDisplacement and k in replaceable:
                        if not k in fixups:
                            logging.debug(f"--FIXUP  key {k}")
                            fixups.append(k)
                        sample[k] = 0
                    else:
                        #logging.warning(f"--MISSING {i} key {k} ")
                        sampleOK = False
                        missingValues += 1
            except Exception as e:
                sampleOK = False
                logging.debug(f"sample={i} key={k} e={e}, skipping")
                missingValues += 1

        if sampleOK:
            samples.append(sample)

    logging.debug((f"samples used={len(samples)}, invalid samples="
                   f"{invalidSamples}, skipped header keys={missingHdrKeys},"
                   f" missing values={missingValues}"))

    codes_release(ibufr)
    return header, samples


def wind_to_UV(windSpeed, windDirection):
    u = -windSpeed * sin(rad * windDirection)
    v = -windSpeed * cos(rad * windDirection)
    return u, v


def gen_id(h):
    bn = h.get('blockNumber', 2147483647)
    sn = h.get('stationNumber', 2147483647)

    if (bn != 2147483647 and sn != 2147483647):
        return ("wmo", f'{bn:02d}{sn:03d}')

    if 'shipOrMobileLandStationIdentifier' in h:
        id = h['shipOrMobileLandStationIdentifier']
        # if it looks remotely like an id...
        if not any(p in id for p in punctuation):
            return ("mobile", h['shipOrMobileLandStationIdentifier'])

    return ("location", f"{h['latitude']:.3f}:{h['longitude']:.3f}")


def add_if_present(d, h, name, bname):
    if bname in h:
        d[name] = h[bname]


def convert_to_geojson(args, h, samples):
    takeoff = datetime(year=h['year'],
                       month=h['month'],
                       day=h['day'],
                       minute=h['minute'],
                       hour=h['hour'],
                       second=h['second'],
                       tzinfo=None)

    typ, id = gen_id(h)

    ts = ciso8601.parse_datetime(h['typicalDate'] + " " + h['typicalTime'] + "-00:00").timestamp()

    properties = {
        "station_id":  id,
        "id_type":  typ,
        "source": "BUFR",
        "path_source": "origin",
        "syn_timestamp": ts,
        "firstSeen": takeoff.timestamp(),
        "lat": h['latitude'],
        "lon": h['longitude'],
    }
    add_if_present(properties, h, "sonde_type", 'radiosondeType')
    add_if_present(properties, h, "sonde_serial", 'radiosondeSerialNumber')
    add_if_present(properties, h, "sonde_frequency",
                   'radiosondeOperatingFrequency')

    # try hard to determine a reasonable takeoff elevation value
    if 'height' in h:
        properties['elevation'] = h['height']
    elif 'heightOfStationGroundAboveMeanSeaLevel' in h:
        properties['elevation'] = h['heightOfStationGroundAboveMeanSeaLevel']
    elif 'heightOfBarometerAboveMeanSeaLevel' in h:
        properties['elevation'] = h['heightOfBarometerAboveMeanSeaLevel']
    else:
        # take height of first sample
        gph = samples[0]['nonCoordinateGeopotentialHeight']
        properties['elevation'] = geopotential_height_to_height(gph)

    fc = geojson.FeatureCollection([])
    fc.properties = properties
    lat_t = fc.properties['lat']
    lon_t = fc.properties['lon']
    firstSeen = fc.properties['firstSeen']
    previous_elevation = fc.properties['elevation'] - args.hstep

    for s in samples:
        lat = lat_t + s['latitudeDisplacement']
        lon = lon_t + s['longitudeDisplacement']
        gpheight = s['nonCoordinateGeopotentialHeight']

        delta = timedelta(seconds=s['timePeriod'])
        sampleTime = takeoff + delta

        height = geopotential_height_to_height(gpheight)
        if height < previous_elevation + args.hstep:
            continue
        previous_elevation = height

        u, v = wind_to_UV(s['windSpeed'], s['windDirection'])

        properties = {
            "time": sampleTime.timestamp(),
            "gpheight": gpheight,
            "temp": s['airTemperature'],
            "dewpoint": s['dewpointTemperature'],
            "pressure": s['pressure'],
            "wind_u": u,
            "wind_v": v
        }
        f = geojson.Feature(geometry=geojson.Point((lon, lat, height)),
                            properties=properties)
        fc.features.append(f)
    fc.properties['lastSeen'] = sampleTime.timestamp()

    duration = fc.properties['lastSeen'] - fc.properties['firstSeen']
    if duration > MAX_FLIGHT_DURATION:
        logging.error(
            f"--- unreasonably long flight: {(duration/3600):.1f} hours")

    return fc


def bufr_qc(args, h, s, fn, zip):
    if len(s) < 10:
        logging.info(f'QC: skipping {fn} from {zip} - only {len(s)} samples')
        return False

    # QC here!
    if not ({'year', 'month', 'day', 'minute', 'hour'} <= h.keys()):
        logging.info(f'QC: skipping {fn} from {zip} - day/time missing')
        return False

    if not 'second' in h:
        h['second'] = 0  # dont care

    return True


def write_geojson(args, source, fc, fn, zip, updated_stations):
    fc.properties['origin_member'] = PurePath(fn).name
    if zip:
        fc.properties['origin_archive'] = PurePath(zip).name
    station_id = fc.properties['station_id']

    if args.station and args.station != station_id:
        return

    logging.debug(f'output samples retained: {len(fc.features)}, station id={station_id}')

    updated_stations.append((station_id, fc.properties))

    cext = ""
    if args.brotli:
        cext = ".br"

    cc = station_id[:2]
    subdir = station_id[2:5]

    syn_time = datetime.utcfromtimestamp(
        fc.properties['syn_timestamp']).replace(tzinfo=pytz.utc)
    day = syn_time.strftime("%Y%m%d")
    time = syn_time.strftime("%H%M%S")

    dest = f'{args.destdir}/{source}/{cc}/{subdir}/{station_id}_{day}_{time}.geojson{cext}'
    ref = f'{source}/{cc}/{subdir}/{station_id}_{day}_{time}.geojson'

    path = Path(dest).parent.absolute()
    Path(path).mkdir(parents=True, exist_ok=True)

    if not fc.is_valid:
                    #print(f"level={n} s={secsIntoFlight:.0f} {height:.1f}m p={pn} lon_t={lon_t} lat_t={lat_t} u={u} v={v} du={du:.1f} dv={dv:.1f} ", file=sys.stderr)

        logging.error(f'--- invalid GeoJSON! {fc.errors()}')
    try:
        gj = geojson.dumps(fc, allow_nan=False).encode("utf8")
    except ValueError as e:
        print("ref", ref)
        print(fc)
        raise

    if args.geojson:
        logging.debug(f'writing {dest}')
        with open(dest, 'wb') as gjfile:
            cmp = gj
            if args.brotli:
                cmp = brotli.compress(gj)
            gjfile.write(cmp)

    fc.properties['path'] = ref # ?????


    if args.dump_geojson:
        pprint(fc)
    return True


def gen_output(args, source, h, s, fn, zip, updated_stations):
    h['samples'] = s

    fc = convert_to_geojson(args, h, s)
    return write_geojson(args, source, fc, fn, zip, updated_stations)


def process_bufr(args, source, f, fn, zip, updated_stations):
    try:
        (h, s) = bufr_decode(f, fn, zip, args)

    # except Exception as e:
    #     logging.info(f"exception processing {fn} from {zip}: {e}")
    #
    except BufrUnreadableError as e:
        logging.warning(f"e={e}")
        return False

#    except CodesInternalError  as err:
    except Exception as err:
        traceback.print_exc(file=sys.stderr)
        return False

    else:
        if bufr_qc(args, h, s, fn, zip):
            return gen_output(args, source, h, s, fn, zip, updated_stations)
    return True


def read_summary(fn):
    if os.path.exists(fn):
        with open(fn, 'rb') as f:
            s = f.read()
            if fn.endswith('.br'):
                s = brotli.decompress(s)
            summary = geojson.loads(s.decode())
            logging.debug(
                f"read summary from {fn} (brotli={fn.endswith('.br')})")
    else:
        logging.debug(f'no summary file yet: {fn}')
        summary = dict()
    return summary


def now():
    return datetime.utcnow().timestamp()

def update_geojson_summary(args, stations, updated_stations, summary):

    stations_with_ascents = dict()
    # unroll into dicts for quick access
    if 'features' in summary:
        for feature in summary.features:
            a = feature.properties['ascents']
            if len(a):
                st_id = a[0]['station_id']
                stations_with_ascents[st_id] = feature

    # remove entries from ascents which have a syn_timestamp less than cutoff_ts
    cutoff_ts = now() - args.max_age

    # now walk the updates
    for id, asc in updated_stations:
        if id in stations_with_ascents:

            # we already have ascents from this station.
            # append, sort by synoptic time and de-duplicate
            oldlist = stations_with_ascents[id]['properties']['ascents']
            oldlist.append(asc)

            pruned = [ x for x in  oldlist if x['syn_timestamp'] > cutoff_ts]

            logging.debug(f"pruning {id}: {len(oldlist)} -> {len(pruned)}")

            newlist = sorted(pruned, key=itemgetter(
                'syn_timestamp'), reverse=True)
            # https://stackoverflow.com/questions/9427163/remove-duplicate-dict-in-list-in-python
            seen = set()
            dedup = []
            for d in newlist:
                # keep an ascent of each source, even if same synop time
                t = str(d['syn_timestamp']) + d['source']
                if t not in seen:
                    seen.add(t)
                    dedup.append(d)
            stations_with_ascents[id]['properties']['ascents'] = dedup
        else:
            # station appears with first-time ascent
            properties = dict()
            properties["ascents"] = [asc]

            if id in stations:
                st = stations[id]
                coords= (st['lon'], st['lat'], st['elevation'])
                properties["name"] = st['name']
            else:
                # unlisted station: anonymous + mobile
                # take coords and station_id as name from ascent
                coords= (asc['lon'], asc['lat'], asc['elevation'])
                properties["name"] = asc['station_id']

            stations_with_ascents[id] = geojson.Feature(geometry=geojson.Point(coords),
                                                        properties=properties)

    # create GeoJSON summary
    ns = na = 0
    fc = geojson.FeatureCollection([])
    for st, f in stations_with_ascents.items():
        ns += 1
        na += len(f.properties['ascents'])
        fc.features.append(f)

    gj = geojson.dumps(fc, indent=4)
    dest = os.path.splitext(args.summary)[0]
    if not dest.endswith(".br"):
        dest += '.br'

    logging.debug(f"summary {dest}: {ns} active stations, {na} ascents")

    fd, path = tempfile.mkstemp(dir=args.tmpdir)
    src = gj.encode("utf8")
    start = time.time()
    dst = brotli.compress(src, quality=BROTLI_SUMMARY_QUALITY)
    end = time.time()
    dt = end-start
    sl = len(src)
    dl = len(dst)
    ratio = (1. - dl/sl)*100.
    logging.debug(f"summary {dest}: brotli {sl} -> {dl}, compression={ratio:.1f}% in {dt:.3f}s")
    os.write(fd, dst)
    os.fsync(fd)
    os.close(fd)
    os.rename(path, dest)
    os.chmod(dest, 0o644)


def winds_to_UV(windSpeeds, windDirection):
    u = []
    v = []
    for i, wdir in enumerate(windDirection):
        rad = 4.0 * np.arctan(1) / 180.
        u.append(-windSpeeds[i] * np.sin(rad * wdir))
        v.append(-windSpeeds[i] * np.cos(rad * wdir))
    return np.array(u), np.array(v)


def basic_qc(Ps, T, Td, U, V):
    # remove the weird entries that give TOA pressure at the start of the array
    Ps = np.round(Ps[np.where(Ps > 100)], 2)
    T = np.round(T[np.where(Ps > 100)], 2)
    Td = np.round(Td[np.where(Ps > 100)], 2)
    U = np.round(U[np.where(Ps > 100)], 2)
    V = np.round(V[np.where(Ps > 100)], 2)

    U[np.isnan(U)] = -9999
    V[np.isnan(V)] = -9999
    Td[np.isnan(Td)] = -9999
    T[np.isnan(T)] = -9999
    Ps[np.isnan(Ps)] = -9999

    if T.size != 0:
        if T[0] < 200 or T[0] > 330 or np.isnan(T).all():
            Ps = np.array([])
            T = np.array([])
            Td = np.array([])
            U = np.array([])
            V = np.array([])

    if not isinstance(Ps, list):
        Ps = Ps.tolist()
    if not isinstance(T, list):
        T = T.tolist()
    if not isinstance(Td, list):
        Td = Td.tolist()
    if not isinstance(U, list):
        U = U.tolist()
    if not isinstance(V, list):
        V = V.tolist()

    return Ps, T, Td, U, V


def RemNaN_and_Interp(raob, file):
    P_allstns = []
    T_allstns = []
    Td_allstns = []
    times_allstns = []
    U_allstns = []
    V_allstns = []
    wmo_ids_allstns = []
    relTime_allstns = []
    sondTyp_allstns = []
    staLat_allstns = []
    staLon_allstns = []
    staElev_allstns = []

    for i, stn in enumerate(raob['Psig']):
        Ps = raob['Psig'][i]
        Ts = raob['Tsig'][i]
        Tds = raob['Tdsig'][i]
        Tm = raob['Tman'][i]
        Tdm = raob['Tdman'][i]
        Pm = raob['Pman'][i]
        Ws = raob['Wspeed'][i]
        Wd = raob['Wdir'][i]

        if len(Pm) > 10 and len(Ps) > 10:
            u, v = winds_to_UV(Ws, Wd)

            PmTm = zip(Pm, Tm)
            PsTs = zip(Ps, Ts)
            PmTdm = zip(Pm, Tdm)
            PsTds = zip(Ps, Tds)

            PT = []
            PTd = []
            for pmtm in PmTm:
                PT.append(pmtm)
            for psts in PsTs:
                PT.append(psts)
            for pmtdm in PmTdm:
                PTd.append(pmtdm)
            for pstds in PsTds:
                PTd.append(pstds)

            PT = [x for x in PT if all(i == i for i in x)]
            PTd = [x for x in PTd if all(i == i for i in x)]

            PT = sorted(PT, key=lambda x: x[0])
            PT = PT[::-1]
            PTd = sorted(PTd, key=lambda x: x[0])
            PTd = PTd[::-1]

            if len(PT) != 0 and len(PTd) > 10:
                P, T = zip(*PT)
                Ptd, Td = zip(*PTd)
                P = np.array(P)
                P = P.astype(int)
                T = np.array(T)
                Td = np.array(Td)

                try:
                    f = interp1d(Ptd, Td, kind='linear', fill_value="extrapolate")
                    Td = f(P)
                except FloatingPointError as e:
                    logging.info(f"station {raob['wmo_ids'][i]} i={i} {e}, file={file} - skipping Ptd, Td")
                    # logging.info(f"Ptd={len(Ptd)} Td={len(Td)} Tdx={len(Tdx)}")
                    # logging.info(f"Ptd={list(Ptd)} Td={list(Td)} P={list(P)}")

                    raise
                    # continue

                try:
                    f = interp1d(Pm, u, kind='linear', fill_value="extrapolate")
                    U = f(P)
                except FloatingPointError as e:
                    logging.info(f"station {raob['wmo_ids'][i]} i={i} {e}, file={file} - skipping Pm, u")
                    #logging.info(f"Pm={Pm} u={u} P={P}")
                    raise

                try:
                    f = interp1d(Pm, v, kind='linear', fill_value="extrapolate")
                    V = f(P)
                except FloatingPointError as e:
                    logging.info(f"station {raob['wmo_ids'][i]} i={i} {e}, file={file} - skipping Pm, v")
                    #logging.info(f"Pm={Pm} v={v} P={P}")
                    raise

                U = U * 1.94384
                V = V * 1.94384

                Pqc, Tqc, Tdqc, Uqc, Vqc = basic_qc(P, T, Td, U, V)

                if len(Pqc) != 0:
                    P_allstns.append(Pqc)
                    T_allstns.append(Tqc)
                    Td_allstns.append(Tdqc)
                    U_allstns.append(Uqc)
                    V_allstns.append(Vqc)
                    wmo_ids_allstns.append(raob['wmo_ids'][i])
                    times_allstns.append(raob['times'][i])
                    relTime_allstns.append(raob['relTime'][i])
                    sondTyp_allstns.append(raob['sondTyp'][i])
                    staLat_allstns.append(raob['staLat'][i])
                    staLon_allstns.append(raob['staLon'][i])
                    staElev_allstns.append(raob['staElev'][i])

    return (relTime_allstns, sondTyp_allstns, staLat_allstns, staLon_allstns, staElev_allstns,
            P_allstns, T_allstns, Td_allstns, U_allstns, V_allstns, wmo_ids_allstns, times_allstns)

# very simplistic


def height2time(h0, height):
    hdiff = height - h0
    return hdiff / ASCENT_RATE


def latlonPlusDisplacement(lat=0, lon=0, u=0, v=0):
    # HeidiWare
    dLat = v / mperdeg
    dLon = u / (cos((lat + dLat / 2) / 180 * pi) * mperdeg)
    return lat + dLat, lon + dLon


def height_to_geopotential_height(height):
    return earth_gravity / ((1 / height) + 1 / earth_avg_radius) / earth_gravity


def emit_ascents(args, source, file, archive, raob, stations, updated_stations):
    relTime, sondTyp, staLat, staLon, staElev, P, T, Td, U, V, wmo_ids, times = RemNaN_and_Interp(
        raob, file)

    for i, stn in enumerate(wmo_ids):
        if args.station and args.station != stn:
            continue
        if stn in stations:
            station = stations[stn]
            if isnan(staLat[i]):
                staLat[i] = station['lat']
            if isnan(staLon[i]):
                staLon[i] = station['lon']
            if isnan(staElev[i]):
                staElev[i] = station['elevation']
        else:
            station = None

        if isnan(staLat[i]) or isnan(staLon[i]) or isnan(staElev[i]):
            logging.error(f"skipping station {stn} - no location")
            continue

        #print(i, stn)
        takeoff = datetime.utcfromtimestamp(
            relTime[i]).replace(tzinfo=pytz.utc)
        syntime = times[i]
        properties = {
            "station_id":  stn,
            "id_type":  "wmo",
            "source":  "netCDF",
            "sonde_type": int(sondTyp[i]),
            "path_source": "simulated",
            "syn_timestamp": syntime.timestamp(),
            "firstSeen": float(relTime[i]),
            "lat": float(staLat[i]),
            "lon": float(staLon[i]),
            "elevation": float(staElev[i]),
        }
        fc = geojson.FeatureCollection([])
        fc.properties = properties

        lat_t = staLat[i]
        lon_t = staLon[i]
        previous_elevation = fc.properties['elevation'] - 100  # args.hstep

        t0 = T[i][0]
        p0 = P[i][0]
        h0 = staElev[i]

        prevSecsIntoFlight = 0
        for n in range(0, len(P[i])):
            pn = P[i][n]

            if isinf(T[i][n]) or isinf(Td[i][n]) or isinf(P[i][n]):
                logging.debug(f"station {stn}: skipping layer  P={P[i][n]}  T={T[i][n]} Td={Td[i][n]}")
                continue

            # gross haque to determine rough time of sample
            height = round(barometric_equation_inv(h0, t0, p0, pn), 1)
            secsIntoFlight = height2time(h0, height)
            delta = timedelta(seconds=secsIntoFlight)
            sampleTime = takeoff + delta

            properties = {
                "time": sampleTime.timestamp(),
                "gpheight": round(height_to_geopotential_height(height), 1),
                "temp": round(T[i][n], 2),
                "dewpoint": round(Td[i][n], 2),
                "pressure": P[i][n],
            }
            u = U[i][n]
            v = V[i][n]
            du = dv = 0
            if u > -9999.0 and v > -9999.0:
                properties["wind_u"] = u
                properties["wind_v"] = v
                dt = secsIntoFlight - prevSecsIntoFlight
                du = u * dt
                dv = v * dt
                lat_t, lon_t = latlonPlusDisplacement(
                    lat=lat_t, lon=lon_t, u=du, v=dv)
                prevSecsIntoFlight = secsIntoFlight

            #print(f"level={n} s={secsIntoFlight:.0f} {height:.1f}m p={pn} lon_t={lon_t} lat_t={lat_t} u={u} v={v} du={du:.1f} dv={dv:.1f} ", file=sys.stderr)

            f = geojson.Feature(geometry=geojson.Point((float(lon_t), float(lat_t), height)),
                                properties=properties)

            if not f.is_valid:
                logging.error(f'--- invalid GeoJSON! {f.errors()}')

            fc.features.append(f)
        fc.properties['lastSeen'] = sampleTime.timestamp()
        write_geojson(args,  source, fc, file, archive, updated_stations)
    return True

def process_netcdf(args, source, file, archive, stationdict, updated_stations):

    with gzip.open(file, 'rb') as f:
        nc = Dataset('inmemory.nc', memory=f.read())

        relTime = nc.variables['relTime'][:].filled(fill_value=np.nan)
        sondTyp = nc.variables['sondTyp'][:].filled(fill_value=np.nan)
        staLat = nc.variables['staLat'][:].filled(fill_value=np.nan)
        staLon = nc.variables['staLon'][:].filled(fill_value=np.nan)
        staElev = nc.variables['staElev'][:].filled(fill_value=np.nan)

        Tman = nc.variables['tpMan'][:].filled(fill_value=np.nan)
        DPDman = nc.variables['tdMan'][:].filled(fill_value=np.nan)
        wmo_ids = nc.variables['wmoStaNum'][:].filled(fill_value=np.nan)

        DPDsig = nc.variables['tdSigT'][:].filled(fill_value=np.nan)
        Tsig = nc.variables['tpSigT'][:].filled(fill_value=np.nan)
        synTimes = nc.variables['synTime'][:].filled(fill_value=np.nan)
        Psig = nc.variables['prSigT'][:].filled(fill_value=np.nan)
        Pman = nc.variables['prMan'][:].filled(fill_value=np.nan)

        Wspeed = nc.variables['wsMan'][:].filled(fill_value=np.nan)
        Wdir = nc.variables['wdMan'][:].filled(fill_value=np.nan)
        raob = {
            "relTime": relTime,
            "sondTyp": sondTyp,
            "staLat": staLat,
            "staLon": staLon,
            "staElev": staElev,

            "Tsig": Tsig,
            "Tdsig": Tsig - DPDsig,
            "Tman": Tman,
            "Psig": Psig,
            "Pman": Pman,
            "Tdman": Tman - DPDman,
            "Wspeed": Wspeed,
            "Wdir": Wdir,
            "times": [datetime.utcfromtimestamp(tim).replace(tzinfo=pytz.utc) for tim in synTimes],
            "wmo_ids": [str(id).zfill(5) for id in wmo_ids]
        }
        return emit_ascents(args, source, file, archive, raob, stationdict, updated_stations)


def newer(filename, ext):
    """
        given a file like foo.ext and an extension like .json,
        return True if:
            foo.json does not exist or
            foo.json has an older modification time than foo.ext
    """
    (fn, e) = os.path.splitext(filename)
    target = fn + ext
    if not os.path.exists(target):
        return True
    return os.path.getmtime(filename) > os.path.getmtime(target)


def initialize_stations(txt_fn, json_fn):
    US_STATES = ["AK", "AL", "AR", "AZ", "CA", "CO", "CT", "DE", "FL", "GA", "HI", "IA", "ID",
                 "IL", "IN", "KS", "LA", "MA", "MD", "ME", "MI", "MN", "MO", "MS", "MT", "NC",
                 "ND", "NE", "NH", "NJ", "NM", "NV", "NY", "OH", "OK", "OR", "PA", "RI", "SC", "SD",
                 "TN", "TX", "UT", "VA", "VT", "WA", "WI", "WV", "WY"]

    stations = geojson.FeatureCollection([])
    stationdict = dict()
    with open(txt_fn, 'r') as csvfile:
        stndata = csv.reader(csvfile, delimiter='\t')
        for row in stndata:
            m = re.match(
                r"(?P<stn_wmoid>^\w+)\s+(?P<stn_lat>\S+)\s+(?P<stn_lon>\S+)\s+(?P<stn_altitude>\S+)(?P<stn_name>\D+)", row[0])
            fields = m.groupdict()
            stn_wmoid = fields['stn_wmoid'][6:]
            stn_name = fields['stn_name'].strip()

            if re.match(r"^[a-zA-Z]{2}\s", stn_name) and stn_name[:2] in US_STATES:
                stn_name = stn_name[2:].strip().title() + ", " + stn_name[:2]
            else:
                stn_name = stn_name.title()
            stn_name = fields['stn_name'].strip().title()
            stn_lat = float(fields['stn_lat'])
            stn_lon = float(fields['stn_lon'])
            stn_altitude = float(fields['stn_altitude'])

            if stn_altitude > -998.8:
                stationdict[stn_wmoid] = {
                    "name":  stn_name,
                    "lat": stn_lat,
                    "lon": stn_lon,
                    "elevation": stn_altitude
                }

        with open(json_fn, 'wb') as jfile:
            j = json.dumps(stationdict, indent=4).encode("utf8")
            jfile.write(j)



def update_station_list(txt_fn):
    """
    fn is expected to look like <path>/station_list.txt
    if a corresponding <path>/station_list.json file exists and is newer:
        read that

    if the corresponding <path>/station_list.json is older or does not exist:
        read and parse the .txt file
        generate the station_list.json
        read that

    return the station fn and dict
    """
    (base, ext) = os.path.splitext(txt_fn)
    if ext != ".txt":
        raise ValueError("expecting .txt extension:", txt_fn)

    json_fn = base + ".json"

    #logging.debug(f'base {base} from {txt_fn}, json_fn {json_fn}')

    # read the station_list.json file
    # create or update on the fly if needed from station_list.txt (same dir assumed)
    if newer(txt_fn, ".json"):
        # rebuild the json file
        logging.debug(f'rebuildin {json_fn} from {txt_fn}')

        initialize_stations(txt_fn, json_fn)
        logging.debug(f'rebuilt {json_fn} from {txt_fn}')

    with open(json_fn, 'rb') as f:
        s = f.read().decode()
        stations = orjson.loads(s)
        logging.debug(f'read stations from {json_fn}')
    return json_fn, stations


def main():
    parser = argparse.ArgumentParser(description='decode radiosonde BUFR and netCDF reports',
                                     add_help=True)
    parser.add_argument('-v', '--verbose', action='store_true', default=False)
    parser.add_argument('--hstep', action='store', type=int, default=100,
                        help="generate output only if samples vary vertically more than hstep")
    parser.add_argument('--destdir', action='store', default=".")
    parser.add_argument('--station', action='store', default=None,
                        help="extract a single station by WMO id")
    parser.add_argument('--geojson', action='store_true', default=False)
    parser.add_argument('--dump-geojson', action='store_true', default=False)
    parser.add_argument('--brotli', action='store_true', default=False)
    parser.add_argument('--summary',
                        action='store',
                        required=True)
    parser.add_argument('-n', '--ignore-timestamps', action='store_true',
                        help="ignore, and do not create timestamps")
    parser.add_argument('--stations', action='store',
                        required=True,
                        help="path to station_list.txt file")
    parser.add_argument('--tmpdir', action='store', default="/tmp")
    parser.add_argument('--max-age', action='store', type=int, default=MAX_ASCENT_AGE_IN_SUMMARY)
    parser.add_argument('files', nargs='*')

    args = parser.parse_args()

    level = logging.WARNING
    if args.verbose:
        level = logging.DEBUG

    # f = OneLineExceptionFormatter('%(asctime)s|%(levelname)s|%(message)s|', '%m/%d/%Y %I:%M:%S %p')
    # root = logging.getLogger()
    # root.setLevel(level)
    # root.setFormatter(f)

    logging.basicConfig(level=level)
    os.umask(0o22)

    station_fn, station_dict = update_station_list(args.stations)
    updated_stations = []
    summary = read_summary(args.summary)
    if not summary:
        # try brotlified version
        summary = read_summary(args.summary + ".br")

    for f in args.files:

        if not args.ignore_timestamps and not newer(f, ".timestamp"):
            logging.debug(f"skipping: {f}  (timestamp)")
            continue

        (fn, ext) = os.path.splitext(f)
        logging.debug(f"processing: {f} fn={fn} ext={ext}")

        if ext == '.zip':  # a zip archive of BUFR files
            with zipfile.ZipFile(f) as zf:
                source = "gisc"
                for info in zf.infolist():
                    try:
                        logging.debug(f"reading: {f} member {info.filename}")
                        data = zf.read(info.filename)
                        fd, path = tempfile.mkstemp(dir=args.tmpdir)
                        os.write(fd, data)
                        os.lseek(fd, 0, os.SEEK_SET)
                        file = os.fdopen(fd)
                    except KeyError:
                        log.error(
                            f'ERROR: zip file {f}: no such member {info.filename}')
                        continue
                    else:
                        logging.debug(
                            f"processing BUFR: {f} member {info.filename}")
                        success = process_bufr(args, source, file, info.filename, f, updated_stations)
                        logging.info(f"----- success={success}")

                        file.close()
                        os.remove(path)
                        if success and not args.ignore_timestamps:
                            Path(fn + ".timestamp").touch(mode=0o777, exist_ok=True)

                        # move to failed

        elif ext == '.bin':   # a singlle BUFR file
            source = "gisc"
            file = open(f, 'rb')
            logging.debug(f"processing BUFR: {f}")

            success = process_bufr(args, source, file, f, None, updated_stations)
            file.close()
            if success and not args.ignore_timestamps:
                Path(fn + ".timestamp").touch(mode=0o777, exist_ok=True)
            # move to failed

        elif ext == '.gz':  # a gzipped netCDF file
            source = "madis"
            logging.debug(f"processing netCDF: {f}")
            success = process_netcdf(args, source, f, None, station_dict, updated_stations)
            logging.info(f"----- success={success}")
            if success and not args.ignore_timestamps:
                Path(fn + ".timestamp").touch(mode=0o777, exist_ok=True)
            # move to failed? do not think so

    # Migrate to all-Geojson
    if updated_stations:
        logging.debug(f"creating GeoJSON summary: {args.summary}")
        update_geojson_summary(args, station_dict, updated_stations, summary)



if __name__ == "__main__":
    sys.exit(main())
