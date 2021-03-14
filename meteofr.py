import requests
import datetime
import pytz
import sys
fr_stations = [
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

x = datetime.datetime(2018, 9, 15)


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
            with open(f"{station}.{ds}{hr}.bfr", "wb") as f:
                f.write(r.content)
                
        
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
