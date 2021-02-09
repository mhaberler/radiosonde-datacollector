from watchgod import watch, RegExpWatcher, Change
import time
import logging
from datetime import datetime, timezone, timedelta, date
import sys, os

main = sys.modules['__main__']

import importlib.util
spec = importlib.util.spec_from_file_location("config", "./config.py")
cf = importlib.util.module_from_spec(spec)
spec.loader.exec_module(cf)


def process(changetype, file, **kwargs):
    print("process:", changetype, file, kwargs)

def cleanup(**kwargs):
    print("cleanup:", kwargs)

def now():
    return datetime.utcnow().timestamp()

def null(changetype, file, **kwargs):
    print("null action called:", kwargs)
    pass

for c in cf.watchconfig:
    c['watch'] = RegExpWatcher(root_path=c['dir'], re_files=c['pattern'])
    c['lastcheck'] = now()


while True:
    for c in cf.watchconfig:
        change = c['watch'].check()
        if change != set():
            for ch in change:
                changetype, file = ch
                if changetype in c['trigger']:
                    result = getattr(main, c['action'])(changetype, file, **c)
        if (c['lastcheck'] + c['cleanup_every']) < now():
            result = getattr(main, c['cleanup'])(changetype, file, **c)
            c['lastcheck'] = now()

        time.sleep(1)
