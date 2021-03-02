import json
import logging
import os
import tempfile
import time
from datetime import datetime


import brotli

import config

import geojson


def now():
    return int(datetime.utcnow().timestamp())


def age(filename):
    if not os.path.exists(filename):
        return 0
    return os.path.getmtime(filename)


def read_file(name, useBrotli=False):
    with open(name, "rb") as f:
        s = f.read()
        sl = len(s)
        if useBrotli:
            s = brotli.decompress(s)
            ratio = (sl / len(s)) * 100.0
            logging.debug(f"w {name}: brotli {sl} -> {len(s)}, {ratio:.1f}%")
        return s


def read_json_file(name, useBrotli=False, asGeojson=False):
    s = read_file(name, useBrotli=useBrotli).decode()
    if asGeojson:
        return geojson.loads(s)
    else:
        return json.loads(s)


def write_json_file(d, name, useBrotli=False, asGeojson=False):
    if asGeojson:
        b = geojson.dumps(d, indent=config.INDENT).encode(config.CHARSET)
    else:
        b = json.dumps(d, indent=config.INDENT).encode(config.CHARSET)
    write_file(b, name, useBrotli=useBrotli)


def write_file(s, name, useBrotli=False):
    fd, path = tempfile.mkstemp(dir=config.tmpdir)
    if useBrotli:
        sl = len(s)
        start = time.time()
        s = brotli.compress(s, quality=config.BROTLI_SUMMARY_QUALITY)
        end = time.time()
        dt = end - start
        dl = len(s)
        ratio = (1.0 - dl / sl) * 100.0
        logging.debug(
            f"w {name}: brotli {sl} -> {dl},"
            f" compression={ratio:.1f}% in {dt:.3f}s"
        )
    os.write(fd, s)
    os.fsync(fd)
    os.close(fd)
    os.rename(path, name)
    os.chmod(name, 0o644)
