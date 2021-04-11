import argparse
import logging
import os
import re
import sys
import time
import pathlib
import pidfile
from subprocess import Popen, PIPE, STDOUT
import shlex

import config


REMOTE_HOST = "madis-data.ncep.noaa.gov"
REMOTE_DIR = "point/raob/netcdf/"
USER = "anonymous"
PASS = "mah@mah.priv.at"
LFTP = "/usr/bin/lftp"
simtime = 3

def process(args):

    cmdline = (f"{LFTP} -d -u {USER},{PASS} -e 'mirror --parallel=4 --verbose "
               f"/{REMOTE_DIR} {config.SPOOLDIR_NOAA_MADIS}; bye' {REMOTE_HOST}")

    command = shlex.split(cmdline)
    logging.debug(f"command: {command}")

    process = Popen(command, stdout=PIPE, stderr=STDOUT)
    stdout, stderr = process.communicate()

    if stdout:
        for l in stdout.decode().split('\n'):
            logging.debug(f"lftp STDOUT: {l}")

    if stderr:
        for l in stderr.decode().split('\n'):
            logging.debug(f"lftp STDERR: {l}")

    return process.returncode


def main():

    chan = config.channels["noaa-madis"]
    lockfile = chan["feedlock"]

    parser = argparse.ArgumentParser(
        description="mirror MADIS ftp directory under lock", add_help=True
    )
    parser.add_argument("-v", "--verbose", action="store_true", default=False)
    parser.add_argument("-s", "--simulate", action="store_true", default=False,
                       help="do not actually run mirrorftp, just test locking")
    parser.add_argument("-l", "--lockfile", action="store",
                        default=lockfile,
                        help=f"lockfile to use, default {lockfile}",
                        )
    parser.add_argument(
        "-w", "--max-wait",
        action="store",
        type=float,
        default=180,
        help="number of seconds to wait for lockfile before giving up",
    )
    parser.add_argument(
        "-i", "--interval",
        action="store",
        type=float,
        default=5,
        help="check lockfile every interval seconds",
    )

    parser.add_argument("files", nargs="*")
    args = parser.parse_args()
    level = logging.WARNING
    if args.verbose:
        level = logging.DEBUG

    logging.basicConfig(level=level)

    remaining = args.max_wait
    retcode = -1

    while True:
        try:
            with pidfile.Pidfile(lockfile,
                                 log=logging.debug,
                                 warn=logging.debug):
                logging.debug(f"acquired {lockfile}")
                if args.simulate:
                    logging.debug(f"simulate work by sleeping {simtime}s")
                    time.sleep(simtime)
                    retcode = 0
                else:
                    retcode = process(args)
                break

        except pidfile.ProcessRunningException:
            logging.debug(f"{lockfile} locked, waiting for {args.interval}s")

            time.sleep(args.interval)
            remaining -= args.interval

            if remaining < 0:
                logging.info(f"failed to qcquire {lockfile} after {args.max_wait}s, giving up")
                sys.exit(1)

    logging.debug(f"done, rc={retcode}, released {lockfile}")
    sys.exit(retcode)


if __name__ == "__main__":
    sys.exit(main())
