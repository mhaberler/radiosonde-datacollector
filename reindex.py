import pathlib
import brotli
import geojson
import time, sys, os

SPOOLDIR_MADIS = r'/var/spool/madis/'
SPOOLDIR_GISC = r'/var/spool/gisc/'

def walkt_tree(directory, pattern):
    nf = 0
    nc = 0
    nu = 0
    for path in sorted(directory.rglob(pattern)):
        with open(path, mode='rb') as f:
            s = f.read()
            nu += len(s)
            if path.suffix == '.br':
                s = brotli.decompress(s)
                nc += len(s)
            summary = geojson.loads(s.decode())
            nf += 1
    return (nf,nu,nc)

def  main(dirlist):
    ntotal = 0
    for d in dirlist:
        start = time.time()
        nf, nu, nc = walkt_tree(pathlib.Path(d),'*.geojson.br')
        ntotal = ntotal + nf
        if nf == 0:
            continue
        end = time.time()
        dt = end-start
        ratio = (1. - nu/nc)*100.
        print(f"directory {d}:")
        print(f"{nf} files, avg compressed file={nc/nf:.0f},  avg uncompressed file={nu/nf:.0f}")
        print(f"avg compression={ratio:.1f}%, total time {dt:.3f}s  {dt*1000/nf:.3f}ms per file")
    print(f"files in total {ntotal}")

if __name__ == "__main__":
    dirlist = [SPOOLDIR_MADIS, SPOOLDIR_GISC]
    dirlist = ['gisc/', 'madis/']

    sys.exit(main(dirlist))
