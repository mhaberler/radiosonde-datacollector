# from: https://stackoverflow.com/questions/1444790/python-module-for-creating-pid-based-lockfile


import sys, os
import errno

class Pidfile():
    def __init__(self, path, log=sys.stdout.write, warn=sys.stderr.write):
        self.pidfile = path
        self.log = log
        self.warn = warn

    def __enter__(self):
        try:
            self.pidfd = os.open(self.pidfile, os.O_CREAT|os.O_WRONLY|os.O_EXCL)
            self.log(f'locked pidfile {self.pidfile}\n')
        except OSError as e:
            if e.errno == errno.EEXIST:
                pid = self._check()
                if pid:
                    self.pidfd = None
                    raise ProcessRunningException(f'process already running in {self.pidfile} as pid {pid}\n')
                else:
                    os.remove(self.pidfile)
                    self.warn(f'removed staled lockfile {self.pidfile}\n')
                    self.pidfd = os.open(self.pidfile, os.O_CREAT|os.O_WRONLY|os.O_EXCL)
            else:
                raise

        os.write(self.pidfd, str(os.getpid()).encode("utf8"))
        os.close(self.pidfd)
        return self

    def __exit__(self, t, e, tb):
        # return false to raise, true to pass
        if t is None:
            # normal condition, no exception
            self._remove()
            return True
        elif t is ProcessRunningException:
            # do not remove the other process lockfile
            return False
        else:
            # other exception
            if self.pidfd:
                # this was our lockfile, removing
                self._remove()
            return False

    def _remove(self):
        self.log(f'removed pidfile {self.pidfile}\n')
        os.remove(self.pidfile)

    def _check(self):
        """check if a process is still running
the process id is expected to be in pidfile, which should exist.
if it is still running, returns the pid, if not, return False."""
        with open(self.pidfile, 'r') as f:
            try:
                pidstr = f.read()
                pid = int(pidstr)
            except ValueError:
                # not an integer
                self.log(f"not an integer: {pidstr}\n")
                return False
            try:
                os.kill(pid, 0)
            except OSError:
                self.log(f"can't deliver signal to {pid}\n")
                return False
            else:
                return pid

class ProcessRunningException(BaseException):
    pass
