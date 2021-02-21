import re
import requests
from datetime import datetime
import pytz
import json
import geojson
import flatten
import brotli
from pprint import pprint
import pandas as pd
from metpy.units import pandas_dataframe_to_unit_arrays, units

top = "https://radiosonde.mah.priv.at/"
data = "data-dev"
station_list = "static/station_list.json"
sources = ["gisc", "madis"]

reshape_feature = {
    'geometry_coordinates.0': 'lon',
    'geometry_coordinates.1': 'lat',
    'geometry_coordinates.2': 'ele',
    'properties_dewpoint': 'dewpoint',
    'properties_gpheight': 'gpheight',
    'properties_pressure': 'pressure',
    'properties_temp': 'temperature',
    'properties_wind_u': 'u_wind',
    'properties_wind_v': 'v_wind',
}


def ts2utc(ts):
    return datetime.utcfromtimestamp(ts).replace(tzinfo=pytz.utc)


def identity(x):
    return x


def pick(dl, key, conversion=identity):
    return [conversion(d[key]) for d in dl]


class Station(object):

    def __init__(self, station_id, sources=["gisc", "madis"]):

        self.sources = sources
        self._read_stationlist()
        self._get_station(station_id)
        self.region = self.station_id[:2]
        self.ident = self.station_id[2:5]
        self._available()

    def _get_station(self, station_id):
        if station_id in self.stations:
            self.station_id = station_id
            self.station_name = self.stations[station_id]['name']
            print("found by id", station_id, self.station_name)
            return
        for id, desc in self.stations.items():
            if desc['name'] == station_id:
                self.station_id = id
                self.station_name = station_id
                print("found by name", id, station_id)
                return
        print("not found by name or id")
        self.station_id = station_id
        self.station_name = 'unknown'

    def _available(self):
        l = []
        for src in self.sources:
            base = f"{top}/{data}/{src}/{self.region}/{self.ident}/"
            r = requests.get(base)
            if r.status_code == 404:
                continue
            js = r.json()
            for e in js:
                if e['type'] == 'file' and e['name'].endswith(".geojson.br"):
                    wmo_id, d, t, _, _ = re.split('_|\.', e['name'])
                    syn_time = datetime.strptime(
                        d + ' ' + t + 'Z', '%Y%m%d %H%M%S%z')
                    l.append((syn_time, src, e['name']))
        self.temps = sorted(l, key=lambda tup: tup[1], reverse=True)

    def _read_stationlist(self):
        url = f"{top}/{station_list}"

        r = requests.get(url)
        if r.status_code == 404:
            self.stations = []
        else:
            self.stations = r.json()

    def stations(self):
        return self.stations

    def available(self):
        return self.temps

    def as_dataframe(self, index=0, date=None):
        gj = self.as_geojson(index=index, date=date)
        flat = []
        for f in gj["features"]:
            flat.append(flatten.flatten(f, parent_key=False, separator='_'))

        col_names = ['pressure', 'gpheight', 'temperature', 'dewpoint',
              'u_wind', 'v_wind', 'time', 'latitude', 'longitude', 'elevation']
        df = pd.DataFrame(columns=col_names)

        df['time'] = pick(flat, 'properties_time', conversion=ts2utc)

        for k, v in reshape_feature.items():
            df[v] = pick(flat, k)

        if not ("fmt" in gj["properties"] and gj["properties"]["fmt"] > 1):
            df['pressure'] = df['pressure'].div(100)

        df_units = {
            'pressure': 'hPa',
            'gpheight': 'meter',
            'temperature': 'kelvin',
            'dewpoint': 'kelvin',
            'u_wind': 'm/s',
            'v_wind': 'm/s',
            'time': None,
            'latitude': 'degrees',
            'longitude': 'degrees',
            'elevation': 'meter'
        }
        return pandas_dataframe_to_unit_arrays(df, column_units=df_units)

    def as_geojson(self, index=0, date=None):
        a = None
        if date:
            if date.tzinfo is None or date.tzinfo.utcoffset(date) is None:
                date = pytz.utc.localize(date)

            for t in self.temps:
                syn_time, src, name = t
                if syn_time != date:
                    continue
                a = t
            if not a:
                raise ValueError((f"no temp available for station "
                                  f"'{self.station_name}' ({self.station_id}) at {date}"))
        else:
            try:
                a = self.temps[index]
            except IndexError:
                raise ValueError((f"no temp available for station "
                                  f"'{self.station_name}' ({self.station_id}) index {index}"))

        base = f"{top}/{data}/{a[1]}/{self.region}/{self.ident}/{a[2]}"
        r = requests.get(base)
        if r.status_code == 404:
            return None
        return geojson.loads(brotli.decompress(r.content).decode())


if __name__ == "__main__":
    # Vienna/Hohe Warte
    station_id = "11035"

    st = Station(station_id)
    for syn_time, src, fn in st.available():
        print(syn_time, src, fn)

    # pprint(st.stations[station_id])
    dt = datetime(2021, 2, 18, 12)

    gj = st.as_geojson(date=dt)
    # pprint(gj)
    #pprint(flatten.flatten(gj,parent_key=False, separator='_'))
    # for f in gj["features"]:
    #     pprint(flatten.flatten(f, parent_key=False, separator='_'))
    df = st.as_dataframe(date=dt)
    print(df)
    # df = geopandas.GeoDataFrame(gj)

    # df = json_normalize(gj)
    # print(df)
