import requests
import datetime
import pytz
import sys
fr_stations = [
    "07145",
    "07110",
    "07761",
    "07645",
    "07510",
    "89642",
    "61998",
    "78897",
    "81405",
    "61980",
    "91925",
    "91938",
    "91958",
    "91592"
]
site = "https://donneespubliques.meteofrance.fr/donnees_libres/"
prefix = "Bufr/RS_HR/"
dest = "/var/spool/meteo-fr/incoming"

def getascent(station, year, month, day, hour):

    date = datetime.datetime( year, month, day)
    ds = date.strftime("%Y%m%d")
    hr = f"{hour:02d}"
    print(f"{site}{prefix}{station}.{ds}{hr}.bfr")

def fetch_all( year, month, day, hour):
    for station in fr_stations:
        date = datetime.datetime( year, month, day)
        ds = date.strftime("%Y%m%d")
        hr = f"{hour:02d}"
        url = f"{site}{prefix}{station}.{ds}{hr}.bfr"
        print(url)
        r = requests.get(url)
        if r:
            print("headers: ", r.headers)
            if 'Last-Modified' in r.headers:
                with open(f"{dest}/{station}.{ds}{hr}.bfr", "wb") as f:
                    f.write(r.content)
            else:
                print(f "skipping station {station}")
#        (sondehub-3.8) sondehub@mah2:~/radiosonde-datacollector-dev$ python meteofr.py
#https://donneespubliques.meteofrance.fr/donnees_libres/Bufr/RS_HR/07110.2021031400.bfr
# goodheaders = 
# {
#     'Date': 'Sun, 14 Mar 2021 08:45:08 GMT',
#     'Server': 'MFWS',
#     'Last-Modified': 'Sun, 14 Mar 2021 03:00:34 GMT',
#     'ETag': '"2da481-1226e-5bd76563a3880"',
#     'Accept-Ranges':
#     'bytes', 'Content-Type': 'text/plain; charset=ISO-8859-1',
#     'Vary': 'Accept-Encoding',
#     'Content-Encoding': 'gzip',
#     'Content-Disposition': 'attachment',
#     'Keep-Alive': 'timeout=5, max=300',
#     'Connection': 'Keep-Alive',
#     'Transfer-Encoding': 'chunked'
# }
# #https://donneespubliques.meteofrance.fr/donnees_libres/Bufr/RS_HR/91958.2021031400.bfr
# bad_headers =  {
#     'Date': 'Sun, 14 Mar 2021 08:45:12 GMT',
#     'Server': 'MFWS',
#     'Set-Cookie': 'PHPSESSID=7gi5hf61kkcnm6eedpsqen8175; expires=Sun, 14-Mar-2021 09:00:12 GMT; path=/',
#     'Expires': 'Thu, 19 Nov 1981 08:52:00 GMT',
#     'Cache-Control': 'no-store, no-cache, must-revalidate, post-check=0, pre-check=0',
#     'Pragma': 'no-cache',
#     'Vary': 'Accept-Encoding',
#     'Content-Encoding': 'gzip',
#     'Content-Length': '8884',
#     'Content-Type': 'text/html; charset=utf-8',
#     'Keep-Alive': 'timeout=5, max=299',
#     'Connection': 'Keep-Alive'
# }

 

#https://donneespubliques.meteofrance.fr/donnees_libres/Bufr/RS_HR/07761.2021031400.bfr

#d =  datetime.datetime.now(tz=pytz.utc)


#print(d.strftime("%Y%m%d%H"), d.hour)
#getascent("07110", 2021, 3, 14, 0)
#getascent("07510", 2021, 3, 13, 12)
fetch_all(2021,3,14,0)

sys.exit(0)

#https://donneespubliques.meteofrance.fr/donnees_libres/Bufr/RS_HR/07145.2021031312.bfr


url =  'https://donneespubliques.meteofrance.fr/donnees_libres/Pdf/RS/RSDispos.json'
r = requests.get(url)

print()

al = r.json()["Bufr/RS_HR/"]


for s in al:
    station = s["station"]
    name  = s["nom"]
    for d in  s["dates"]:
        day = d["jour"]
        ascents = d["reseaux"]
