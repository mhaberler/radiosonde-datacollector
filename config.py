
FAKE_TIME_STEPS = 30  # assume 30sec update interval
MAX_FLIGHT_DURATION = 3600 * 5  # rather unlikely

# see examples/polyfit.py
# y = 6.31426 * x + -0.00019 * x^2 + -373.12281
ASCENT_RATE = 6.3 # m/s = ca 380m/min

BROTLI_SUMMARY_QUALITY = 11  # 7

# drop ascents older than MAX_ASCENT_AGE_IN_SUMMARY from summary
# the files are kept nevertheless
MAX_ASCENT_AGE_IN_SUMMARY = 7 * 3600 * 24

SPOOLDIR_MADIS = r"/var/spool/madis/"
SPOOLDIR_GISC = r"/var/spool/gisc/"
PROCESSED = r"processed"
FAILED = r"failed"
INCOMING = r"incoming"
TS_PROCESSED = ".processed"
TS_FAILED = ".failed"
TS_TIMESTAMP = ".timestamp"
LOCKFILE = "/var/lock/process-radiosonde.pid"

# after 3 days move to processed
KEEP_MADIS_PROCESSED_FILES = 86400 * 3

# added to featurecollection.properties.fmt = FORMAT_VERSION
FORMAT_VERSION = 3
