import logging
import pathlib

import util


def write_geojson(dest, fc):

    if not fc.is_valid:
        logging.error(f"writing {dest}: invalid GeoJSON {fc.errors()}")
        return False

    path = pathlib.Path(dest).parent.absolute()
    pathlib.Path(path).mkdir(parents=True, exist_ok=True)

    try:
        util.write_json_file(fc, dest, useBrotli=True, asGeojson=True)
    except Exception as e:
        logging.exception(f"writing {dest}: {e}")
        return False
    else:
        return True
