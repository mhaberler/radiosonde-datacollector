from watchgod import watch, RegExpWatcher, Change
import time
import logging
from datetime import datetime, timezone, timedelta, date


def process(changetype, file, **kwargs):
    print("process:", changetype, file, kwargs)


def cleanup(**kwargs):
    print("cleanup:", kwargs)

from config  import watchconfig


def now():
    return datetime.utcnow().timestamp()



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
