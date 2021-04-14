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


simtime = 3

def mirror(cmdline):

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


    parser = argparse.ArgumentParser(
        description="ftp-mirror channels under lock", add_help=True
    )
    parser.add_argument("-v", "--verbose", action="store_true", default=False)
    parser.add_argument("-s", "--simulate", action="store_true", default=False,
                       help="do not actually run mirrorftp, just test locking")
    # parser.add_argument("-l", "--lockfile", action="store",
    #                     default=lockfile,
    #                     help=f"lockfile to use, default {lockfile}",
    #                     )
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
    parser.add_argument(
        "-p", "--parallel",
        action="store",
        type=int,
        default=4,
        help="number of parallel ftp sessions",
    )
    parser.add_argument("channels", nargs="*")
    args = parser.parse_args()
    level = logging.WARNING
    if args.verbose:
        level = logging.DEBUG

    logging.basicConfig(level=level,
                        format='%(levelname)-3.3s:%(module)s:%(funcName)s:%(lineno)d  %(message)s')

    remaining = args.max_wait
    retcode = -1

    for c in args.channels:
        if  c not in config.channels:
            logging.error(f"no such channel: {c}")
            continue

        chan = config.channels[c]
        try:
            ftp_host = chan["ftp-host"]
            remote_dir = chan["remote-dir"]
            local_dir = chan["local-dir"]
            ftp_user = chan["ftp-user"]
            ftp_pass = chan["ftp-pass"]
            ftp_glob = chan["ftp-glob"]
            lockfile = chan["feedlock"]

            vrb = "--verbose" if args.verbose else ""

            cmdline = (f"{config.LFTP} -d -u {ftp_user},{ftp_pass} "
                       f"-e 'mirror --parallel={args.parallel} {vrb} "
                       f" --include-glob={ftp_glob} "
                       f"/{remote_dir} {local_dir}; bye' {ftp_host}")

        except KeyError:
            logging.exception(f"invalid FTP config for channel {c}: {chan}")
            continue

        # good to go
        logging.debug(f"work on: {c} - cmd: {cmdline}")

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
                        retcode = mirror(cmdline)
                    break

            except pidfile.ProcessRunningException:
                logging.debug(f"{lockfile} locked, waiting for {args.interval}s")

                time.sleep(args.interval)
                remaining -= args.interval

                if remaining < 0:
                    logging.info(f"failed to qcquire {lockfile} after {args.max_wait}s, giving up")
                    retcode = -1
                    break

        logging.debug(f"done, rc={retcode}, released {lockfile}")

    sys.exit(retcode)


if __name__ == "__main__":
    sys.exit(main())
