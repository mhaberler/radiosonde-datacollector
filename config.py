
FAKE_TIME_STEPS = 30  # assume 30sec update interval
MAX_FLIGHT_DURATION = 3600 * 5  # rather unlikely

# see examples/polyfit.py
# y = 6.31426 * x + -0.00019 * x^2 + -373.12281
ASCENT_RATE = 6.3 # m/s = ca 380m/min

BROTLI_SUMMARY_QUALITY = 11  # 7

# drop ascents older than MAX_ASCENT_AGE_IN_SUMMARY from summary
# the files are kept nevertheless
MAX_DAYS_IN_SUMMARY = 14

SPOOLDIR_MADIS = r"/var/spool/madis/"
SPOOLDIR_GISC = r"/var/spool/gisc/"
SPOOLDIR_GISC_TOKYO = r"/var/spool/gisc-tokyo/"
PROCESSED = r"processed"
FAILED = r"failed"
INCOMING = r"incoming"
TS_PROCESSED = ".processed"
TS_FAILED = ".failed"
TS_TIMESTAMP = ".timestamp"
LOCKFILE = "/var/lock/process-radiosonde.pid"
DATA_DIR = "data/"
STATIC_DIR = "static/"
WWW_DIR = "/var/www/radiosonde.mah.priv.at/"
STATION_LIST = WWW_DIR + STATIC_DIR + "station_list.json"
MADIS_DATA = WWW_DIR + DATA_DIR + "madis/"
GISC_DATA = WWW_DIR + DATA_DIR + "gisc/"
STATION_TXT = "station_list.txt"
CHARSET = "utf-8"
tmpdir = "/tmp"
INDENT = 4
SUMMARY = "summary.geojson.br"

# after 3 days move to processed
KEEP_MADIS_PROCESSED_FILES = 86400 * 3

# added to featurecollection.properties.fmt = FORMAT_VERSION
# 4 - using deep subdirs year/month under station
FORMAT_VERSION = 5


# set in process.py, read-only
known_stations = {}

# for FM-35 files which do not have track info
# not very reliable, so move to client
GENERATE_PATHS = False
