from watchgod import watch, RegExpWatcher, Change
import time
import logging
from datetime import datetime, timezone, timedelta, date


def process(changetype, file, **kwargs):
    print("process:", changetype, file, kwargs)


def cleanup(**kwargs):
    print("cleanup:", kwargs)


def now():
    return datetime.utcnow().timestamp()


watchconfig = [
    {
        "dir":  "/var/spool/gisc/incoming",
        "type": "BUFR",
        "trigger": [Change.added,  Change.modified],
        "pattern": r"^.*.zip$",
        "action": process,
        "action_args":  {
            "loglevel":  logging.DEBUG,
        },
        "cleanup":  cleanup,
        "cleanup_every":  3600,
        "cleanup_args":  {
            "todir": "/var/spool/gisc/processed",
            "age":  86400 * 7,
        },
    },
    {
        "dir": "/var/spool/madis",
        "type": "netCDF",
        "trigger": [Change.added,  Change.modified],
        "pattern": r"^.*.gz$",
        "action": process,
        "action_args":  {
            "loglevel":  logging.INFO,
        },
        "cleanup": cleanup,
        "cleanup_every":  3600,
        "cleanup_args":  {
            "todir": "/var/spool/madis-processed",
            "age":  86400 * 7,
        },
    },
]

for c in watchconfig:
    c['watch'] = RegExpWatcher(root_path=c['dir'], re_files=c['pattern'])
    c['lastcheck'] = now()

while True:
    for c in watchconfig:
        change = c['watch'].check()
        if change != set():
            for ch in change:
                changetype, file = ch
                if changetype in c['trigger']:
                    c['action'](changetype, file, **c)
        if (c['lastcheck'] + c['cleanup_every']) < now():
            c['cleanup'](**c)
            c['lastcheck'] = now()

        time.sleep(1)
