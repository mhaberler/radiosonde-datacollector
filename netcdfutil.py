import gzip
import logging
from datetime import datetime, timedelta
from math import cos, isinf, isnan, pi

from config import ASCENT_RATE

from constants import earth_avg_radius, earth_gravity, mperdeg

import geojson

from netCDF4 import Dataset

import numpy as np

import pytz

from scipy.interpolate import interp1d

from thermodynamics import barometric_equation_inv

import util

# ASCENT_RATE = 5  # m/s = 300m/min
# earth_gravity = 9.80665
# earth_avg_radius = 6371008.7714
# mperdeg = 111320.0


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

    for i, _stn in enumerate(raob["Psig"]):
        Ps = raob["Psig"][i]
        Ts = raob["Tsig"][i]
        Tds = raob["Tdsig"][i]

        Tm = raob["Tman"][i]
        Tdm = raob["Tdman"][i]
        Pm = raob["Pman"][i]

        Ws = raob["Wspeed"][i]
        Wd = raob["Wdir"][i]

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

                # try:
                #     f = interp1d(Ptd, Td, kind="linear", fill_value="extrapolate")
                #     Td = f(P)
                # except FloatingPointError as e:
                #     logging.info(
                #         f"station {raob['wmo_ids'][i]} i={i} {e}, file={file} - skipping Ptd, Td"
                #     )
                #     # logging.info(f"Ptd={len(Ptd)} Td={len(Td)} Tdx={len(Tdx)}")
                #     # logging.info(f"Ptd={list(Ptd)} Td={list(Td)} P={list(P)}")
                #
                #     raise
                #     # continue
                #
                # try:
                #     f = interp1d(Pm, u, kind="linear", fill_value="extrapolate")
                #     U = f(P)
                # except FloatingPointError as e:
                #     logging.info(
                #         f"station {raob['wmo_ids'][i]} i={i} {e}, file={file} - skipping Pm, u"
                #     )
                #     # logging.info(f"Pm={Pm} u={u} P={P}")
                #     raise
                #
                # try:
                #     f = interp1d(Pm, v, kind="linear", fill_value="extrapolate")
                #     V = f(P)
                # except FloatingPointError as e:
                #     logging.info(
                #         f"station {raob['wmo_ids'][i]} i={i} {e}, file={file} - skipping Pm, v"
                #     )
                #     # logging.info(f"Pm={Pm} v={v} P={P}")
                #     raise

                # U = U * 1.94384
                # V = V * 1.94384

                Pqc, Tqc, Tdqc, Uqc, Vqc = basic_qc(P, T, Td, U, V)

                if len(Pqc) != 0:
                    P_allstns.append(Pqc)
                    T_allstns.append(Tqc)
                    Td_allstns.append(Tdqc)
                    U_allstns.append(Uqc)
                    V_allstns.append(Vqc)
                    wmo_ids_allstns.append(raob["wmo_ids"][i])
                    times_allstns.append(raob["times"][i])
                    relTime_allstns.append(raob["relTime"][i])
                    sondTyp_allstns.append(raob["sondTyp"][i])
                    staLat_allstns.append(raob["staLat"][i])
                    staLon_allstns.append(raob["staLon"][i])
                    staElev_allstns.append(raob["staElev"][i])

    return (
        relTime_allstns,
        sondTyp_allstns,
        staLat_allstns,
        staLon_allstns,
        staElev_allstns,
        P_allstns,
        T_allstns,
        Td_allstns,
        U_allstns,
        V_allstns,
        wmo_ids_allstns,
        times_allstns,
    )


# very simplistic
def height2time(h0, height):
    hdiff = height - h0
    return hdiff / ASCENT_RATE


def emit_ascents(args, source, file, archive, raob, stations):
    (
        relTime,
        sondTyp,
        staLat,
        staLon,
        staElev,
        P,
        T,
        Td,
        U,
        V,
        wmo_ids,
        times,
    ) = RemNaN_and_Interp(raob, file)

    results = []

    for i, stn in enumerate(wmo_ids):
        if args.station and args.station != stn:
            continue
        if stn in stations:
            station = stations[stn]
            if isnan(staLat[i]):
                staLat[i] = station["lat"]
            if isnan(staLon[i]):
                staLon[i] = station["lon"]
            if isnan(staElev[i]):
                staElev[i] = station["elevation"]
        else:
            station = None

        if isnan(staLat[i]) or isnan(staLon[i]) or isnan(staElev[i]):
            logging.error(f"skipping station {stn} - no location")
            continue

        # print(i, stn)
        takeoff = datetime.utcfromtimestamp(relTime[i]).replace(tzinfo=pytz.utc)
        syntime = times[i]
        properties = {
            "station_id": stn,
            "id_type": "wmo",
            "source": "netCDF",
            "sonde_type": int(sondTyp[i]),
            "path_source": "simulated",
            "syn_timestamp": int(syntime.timestamp()),
            "firstSeen": float(relTime[i]),
            "lat": round(float(staLat[i]), 6),
            "lon": round(float(staLon[i]), 6),
            "elevation": round(float(staElev[i]), 1),
        }
        fc = geojson.FeatureCollection([])
        fc.properties = properties

        lat_t = staLat[i]
        lon_t = staLon[i]

        t0 = T[i][0]
        p0 = P[i][0]
        h0 = staElev[i]

        prevSecsIntoFlight = 0

        logging.debug(f"station {stn}: samples={len(P[i])}")
        for n in range(0, len(P[i])):
            pn = P[i][n]

            if isinf(T[i][n]) or isinf(Td[i][n]) or isinf(P[i][n]):
                logging.debug(
                    f"station {stn}: skipping layer  P={P[i][n]}  T={T[i][n]} Td={Td[i][n]}"
                )
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
                properties["wind_u"] = round(u, 2)
                properties["wind_v"] = round(v, 2)
                dt = secsIntoFlight - prevSecsIntoFlight
                du = u * dt
                dv = v * dt
                lat_t, lon_t = latlonPlusDisplacement(lat=lat_t, lon=lon_t, u=du, v=dv)
                prevSecsIntoFlight = secsIntoFlight
            f = geojson.Feature(
                geometry=geojson.Point((float(lon_t), float(lat_t), round(height, 2))),
                properties=properties,
            )
            if not f.is_valid:
                logging.error(f"invalid GeoJSON! {f.errors()}")
                return False, None

            fc.features.append(f)
        fc.properties["lastSeen"] = sampleTime.timestamp()
        results.append((fc, file, archive))
    return True, results


def process_netcdf(args, source, file, archive, config.known_stations,):

    with gzip.open(file, "rb") as f:
        try:
            nc = Dataset("inmemory.nc", memory=f.read())
        except Exception as e:
            logging.error(f"exception {e} reading {f} as netCDF")
            return False, None

        relTime = nc.variables["relTime"][:].filled(fill_value=np.nan)
        sondTyp = nc.variables["sondTyp"][:].filled(fill_value=np.nan)
        staLat = nc.variables["staLat"][:].filled(fill_value=np.nan)
        staLon = nc.variables["staLon"][:].filled(fill_value=np.nan)
        staElev = nc.variables["staElev"][:].filled(fill_value=np.nan)

        Tman = nc.variables["tpMan"][:].filled(fill_value=np.nan)
        DPDman = nc.variables["tdMan"][:].filled(fill_value=np.nan)
        wmo_ids = nc.variables["wmoStaNum"][:].filled(fill_value=np.nan)

        DPDsig = nc.variables["tdSigT"][:].filled(fill_value=np.nan)
        Tsig = nc.variables["tpSigT"][:].filled(fill_value=np.nan)
        synTimes = nc.variables["synTime"][:].filled(fill_value=np.nan)
        Psig = nc.variables["prSigT"][:].filled(fill_value=np.nan)
        Pman = nc.variables["prMan"][:].filled(fill_value=np.nan)

        Wspeed = nc.variables["wsMan"][:].filled(fill_value=np.nan)
        Wdir = nc.variables["wdMan"][:].filled(fill_value=np.nan)
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
            "times": [
                datetime.utcfromtimestamp(tim).replace(tzinfo=pytz.utc)
                for tim in synTimes
            ],
            "wmo_ids": [str(ident).zfill(5) for ident in wmo_ids],
        }
        return emit_ascents(args, source, file, archive, raob, stationdict)
