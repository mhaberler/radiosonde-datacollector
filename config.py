FAKE_TIME_STEPS = 30  # assume 30sec update interval
MAX_FLIGHT_DURATION = 3600 * 5  # rather unlikely

# see examples/polyfit.py
# y = 6.31426 * x + -0.00019 * x^2 + -373.12281
ASCENT_RATE = 6.3  # m/s = ca 380m/min

BROTLI_SUMMARY_QUALITY = 11  # 7

# drop ascents older than MAX_ASCENT_AGE_IN_SUMMARY from summary
# the files are kept nevertheless
MAX_DAYS_IN_SUMMARY = 14

# after 3 days move to processed
KEEP_MADIS_PROCESSED_FILES = 86400 * 3

WWW_DIR = "/var/www/radiosonde.mah.priv.at/"
DATA_DIR = "data/"
STATIC_DIR = "static/"
STATION_TXT = "station_list.txt"
SUMMARY = "summary.geojson.br"
FM94_AREA = "area-fm94.geojson.br"
FM94_BBOX = "bbox-fm94.geojson.br"
FM94_MAXDIST = "maxdist-fm94.geojson.br"

PROCESSED = r"processed"
FAILED = r"failed"
INCOMING = r"incoming"
TS_PROCESSED = ".processed"
TS_FAILED = ".failed"
TS_TIMESTAMP = ".timestamp"
LOCKFILE = "/var/lock/process-radiosonde."
FEED_LOCKFILE = "feedlock.pid"
STATION_LIST = WWW_DIR + STATIC_DIR + "station_list.json"
FM35_DATA = WWW_DIR + DATA_DIR + "fm35/"
FM94_DATA = WWW_DIR + DATA_DIR + "fm94/"
CHARSET = "utf-8"
tmpdir = "/tmp"
INDENT = 4
MESSAGE_START_SIGNATURE = b"BUFR"

# added to featurecollection.properties.fmt = FORMAT_VERSION
# 4 - using deep subdirs year/month under station
FORMAT_VERSION = 7


# set in process.py, read-only
known_stations = {}

# for FM-35 files which do not have track info
# not very reliable, so move to client
GENERATE_PATHS = False

# tag samples with the section source of an FM35 report:
# mandatory, sig temp, sig wind, max wind
# aids in debugging strange values
# "mandatory", "sig_temp", "sig_wind", "max_wind"
TAG_FM35 = True


# minimum vertical distance of samples to be repored
# hires BUFR files have a lot of samples
HSTEP = 100

SPOOLDIR = r"/var/spool/"
SPOOLDIR_NOAA_MADIS = SPOOLDIR + f"noaa-madis/"
SPOOLDIR_GISC_OFFENBACH = SPOOLDIR + r"gisc-offenbach/"
SPOOLDIR_GISC_TOKYO = SPOOLDIR + r"gisc-tokyo/"
SPOOLDIR_GISC_MOSCOW = SPOOLDIR + r"gisc-moscow/"
SPOOLDIR_METEO_FR = SPOOLDIR + r"meteo-fr/"
SPOOLDIR_NOAA_GTS = SPOOLDIR + r"noaa-gts/"

channels = {
    "noaa-gts": {
        "name": "NOAA GTS",
        "spooldir": SPOOLDIR_NOAA_GTS,
        "pattern": ".*\\.(zip|bufr|bfr|bin)$",
        "keeptime": 0,
        "retain" : 14, # days
        "feedlock": SPOOLDIR_NOAA_GTS + FEED_LOCKFILE,
    },
    "gisc-offenbach": {
        "name": "GISC Offenbach",
        "spooldir": SPOOLDIR_GISC_OFFENBACH,
        "pattern": ".*\\.(zip|bufr|bfr|bin)$",
        "keeptime": 0,
        "retain" : 14, # days
        "feedlock": SPOOLDIR_GISC_OFFENBACH + FEED_LOCKFILE,
    },
    "gisc-moscow": {
        "name":  "GISC Moscow",
        "spooldir": SPOOLDIR_GISC_MOSCOW,
        "keeptime": 0,
        "retain" : 14, # days
        "pattern": ".*\\.(zip|bufr|bfr|bin|b|txt)$",
        "feedlock": SPOOLDIR_GISC_MOSCOW + FEED_LOCKFILE,
    },
    "meteo-fr": {
        "name": "Meteo France",
        "spooldir": SPOOLDIR_METEO_FR,
        "keeptime": 0,
        "pattern": ".*\\.(zip|bufr|bfr|bin)$",
        "retain" : 14, # days
        "feedlock": SPOOLDIR_METEO_FR + FEED_LOCKFILE,

        # "stations": [
        #     # 0,12
        #     "07145",  # Paris-Trappes
        #     "07110",  # Brest-Guipavas
        #     "07761",  # Ajaccio
        #     "07645",  # Nîmes-Courbessac
        #     "07510",  # Bordeaux-Mérignac
        #     "61998",  # Kerguelen
        #     "81405",  # Rochambeau
        #     "61980",  # Gillot
        #     "91925",  # Hiva-Oa
        #     "91938",  # Faa'a
        #     "91592",  # Nouméa

        #     "89642",  # Dumont D'Urville - 0h
        #     "78897",  # Le Raizet 12h
        #     "91958",  # Rapa 18h
        # ],
    },
    "gisc-tokyo": {
        "name": "GISC Tokyo",
        "spooldir": SPOOLDIR_GISC_TOKYO,
        "keeptime": 0,
        "pattern": ".*\\.(zip|bufr|bfr|bin)$",
        "retain" : 14, # days
        "feedlock": SPOOLDIR_GISC_TOKYO + FEED_LOCKFILE,
    },
    "noaa-madis": {
        "name": "NOAA MADIS",
        "spooldir": SPOOLDIR_NOAA_MADIS,
        "keeptime": -1,
        "pattern": ".*\\.gz$",
        "retain" : 14, # days
        "feedlock": SPOOLDIR_NOAA_MADIS + FEED_LOCKFILE,
    },
}
