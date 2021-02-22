import re
import requests
import json
import geojson
import brotli
import pandas as pd
from metpy.units import pandas_dataframe_to_unit_arrays, units
from datetime import datetime
import pytz


class Station(object):

    def __init__(self,
                 station_id,
                 sources=["gisc", "madis"],
                 data_dir="data-dev",
                 site="https://radiosonde.mah.priv.at/",
                 station_list="static/station_list.json"
                 ):
        self.sources = sources
        self.data_dir = data_dir
        self.site = site
        self.station_list = station_list
        self._read_stationlist()
        self._get_station(station_id)
        self.region = self.station_id[:2]
        self.ident = self.station_id[2:5]
        self._available()

    def _get_station(self, station_id):
        if station_id in self.stations:
            self.station_id = station_id
            self.station_name = self.stations[station_id]['name']
            return
        for id, desc in self.stations.items():
            if desc['name'] == station_id:
                self.station_id = id
                self.station_name = station_id
                return
        self.station_id = station_id
        self.station_name = 'unknown'

    def _available(self):
        ascents = []
        for src in self.sources:
            base = (f"{self.site}/{self.data_dir}/{src}/"
                    f"{self.region}/{self.ident}/")
            r = requests.get(base)
            if r.status_code == 404:
                continue
            js = r.json()
            for e in js:
                if e['type'] == 'file' and e['name'].endswith(".geojson.br"):
                    wmo_id, d, t, _, _ = re.split('_|\.', e['name'])
                    syn_time = datetime.strptime(
                        d + ' ' + t + 'Z', '%Y%m%d %H%M%S%z')
                    ascents.append((syn_time, src, e['name']))
        self.temps = sorted(ascents, key=lambda tup: tup[1], reverse=True)

    def _read_stationlist(self):
        stationlist = f"{self.site}/{self.station_list}"
        r = requests.get(stationlist)
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

        # pre-version 2 format files had Pa instead of hPa,
        # and no "fmt" attribute
        # normalize on hPa:
        if "fmt" in gj["properties"]:
            pscale=1.
        else:
            pscale=100.
        print("pscale=",pscale)
        flat=[]
        for f in gj["features"]:
            v={}
            v['longitude']=f.geometry.coordinates[0]
            v['latitude']=f.geometry.coordinates[1]
            v['elevation']=f.geometry.coordinates[2]
            v['dewpoint']=f.properties['dewpoint']
            v['gpheight']=f.properties['gpheight']
            v['pressure']=f.properties['pressure']
            v['temperature']=f.properties['temp']
            v['u_wind']=f.properties['wind_u']
            v['v_wind']=f.properties['wind_v']
            v['time']=datetime.utcfromtimestamp(
                f.properties['time']).replace(tzinfo=pytz.utc)
            v['pressure']=f.properties['pressure'] / pscale
            flat.append(v)

        col_names=['pressure', 'gpheight', 'temperature', 'dewpoint',
              'u_wind', 'v_wind', 'time', 'latitude', 'longitude', 'elevation']
        df=pd.DataFrame(flat, columns=col_names)
        units={
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
        gj["properties"]["station_name"]=self.station_name
        return (pandas_dataframe_to_unit_arrays(df,
                                                column_units=units),
                                                gj["properties"])

    def as_geojson(self, index=0, date=None):
        a=None
        if date:
            if date.tzinfo is None or date.tzinfo.utcoffset(date) is None:
                date=pytz.utc.localize(date)
            for t in self.temps:
                syn_time, src, name=t
                if syn_time != date:
                    continue
                a=t
            if not a:
                raise ValueError((f"no temp available for station "
                                  f"'{self.station_name}'"
                                  f" ({self.station_id}) at {date}"))
        else:
            try:
                a=self.temps[index]
            except IndexError:
                raise ValueError((f"no temp available for station "
                                  f"'{self.station_name}' ({self.station_id})"
                                  f" index {index}"))

        base=(f"{self.site}/{self.data_dir}/"
              f"{a[1]}/{self.region}/{self.ident}/{a[2]}")


        r=requests.get(base)
        if r.status_code == 404:
            return None
        return geojson.loads(brotli.decompress(r.content).decode())


if __name__ == "__main__":
    from pprint import pprint

    # select a station - can be a WMO id or the exact
    # station name in station_list.json:
    #
    station_id = "11035"
    #station_id = 'Vienna/Hohe Warte'

    st=Station(station_id)

    # list available ascents (aka 'temps')
    for syn_time, src, fn in st.available():
        print(syn_time, src, fn)

    # retrieve a particular ascent
    dt=datetime(2021, 2, 18, 12)
    gj=st.as_geojson(date=dt)

    # st.as_geojson() defaults to the latest ascent
    # gj = st.as_geojson()

    # retrieve ascent as pandas dataframe with units dictionary,
    # plus ascent metadata
    df, metadata=st.as_dataframe(date=dt)
    pprint(metadata)
    print(df)
