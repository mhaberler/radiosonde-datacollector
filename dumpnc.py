
from math import cos, isinf, isnan, pi

from netCDF4 import Dataset

import numpy as np

import logging


def dump_netcdf(f, station_name, station_ids):    

    try:
        nc = Dataset("inmemory.nc", memory=f.read())
    except Exception as e:
        logging.error(f"exception {e} reading {f} as netCDF")
        return False, None

    recNum = nc.dimensions["recNum"].size
    manLevel = nc.dimensions["manLevel"].size
    sigTLevel = nc.dimensions["sigTLevel"].size
    sigWLevel = nc.dimensions["sigWLevel"].size
    sigPresWLevel = nc.dimensions["sigPresWLevel"].size
    mWndNum = nc.dimensions["mWndNum"].size

    wmo_ids = nc.variables["wmoStaNum"][:].filled(fill_value=np.nan)
    if args.station_ids:
        print(' '.join([str(ident).zfill(5) for ident in wmo_ids]))
        print()
    
    
    for i, stn in enumerate([str(ident).zfill(5) for ident in wmo_ids]):
        if station_name and station_name != stn:
            continue

        # per station properties
        sondTyp = nc.variables["sondTyp"][i].filled(fill_value=np.nan)
        staLat = nc.variables["staLat"][i].filled(fill_value=np.nan)
        staLon = nc.variables["staLon"][i].filled(fill_value=np.nan)
        staElev = nc.variables["staElev"][i].filled(fill_value=np.nan)
        synTime = nc.variables["synTime"][i].filled(fill_value=np.nan)
        relTime = nc.variables["relTime"][i].filled(fill_value=np.nan)

        # pressure, temp, geopot height, spread at mandatory levels
        prMan = nc.variables["prMan"][i].filled(fill_value=np.nan)
        tpMan = nc.variables["tpMan"][i].filled(fill_value=np.nan)
        htMan = nc.variables["htMan"][i].filled(fill_value=np.nan)
        
        # "dew point depression" = "spread" for the rest of us
        tdMan = nc.variables["tdMan"][i].filled(fill_value=np.nan)

        # wind, mandatory at levels
        wsMan = nc.variables["wsMan"][i].filled(fill_value=np.nan)
        wdMan = nc.variables["wdMan"][i].filled(fill_value=np.nan)

        print(f"\nstation {stn}\nmandatory levels ({manLevel}):\nn\tpres\ttemp\tgph\tdpd\tws\twd")
        # mandatory levels must have all of p d t gph, optionally speed dir
        for j in range(manLevel):
            if isnan(prMan[j]) and isnan(tpMan[j]) and isnan(htMan[j]) and isnan(tdMan[j]) and isnan(wsMan[j]) and isnan(wdMan[j]):
                continue            
            print(f"{j}\t{prMan[j]:.1f}\t{tpMan[j]:.1f}\t{htMan[j]:.1f}\t{tdMan[j]:.1f}\t{wsMan[j]:.1f}\t{wdMan[j]:.1f}")


            
        # sig Temp levels have pressure, temp, spread
        prSigT = nc.variables["prSigT"][i].filled(fill_value=np.nan)
        tpSigT = nc.variables["tpSigT"][i].filled(fill_value=np.nan)
        tdSigT = nc.variables["tdSigT"][i].filled(fill_value=np.nan)
        
        print(f"\nstation {stn}\nsig temp levels ({sigTLevel}):\nn\tpres\ttemp\tdpd")       
        # pressure dewpoint temp at sig T levels
        for j in range(sigTLevel):
            if isnan(prSigT[j]) and isnan(tpSigT[j]) and isnan(tdSigT[j]):
                continue
            print(f"{j}\t{prSigT[j]:.1f}\t{tpSigT[j]:.1f}\t{tdSigT[j]:.1f}")


        # sig Wind levels:
        # should have pressure, geopot height, speed, direction
        # the following would be nice to have, but I found them all to
        # be missing (nan) so fill in via height and barometric equation:
        #prSigW = nc.variables["prSigW"][i].filled(fill_value=np.nan)

        htSigW = nc.variables["htSigW"][i].filled(fill_value=np.nan)
        wsSigW = nc.variables["wsSigW"][i].filled(fill_value=np.nan)
        wdSigW = nc.variables["wdSigW"][i].filled(fill_value=np.nan)
     
        print(f"\nstation {stn}\nsig wind levels ({sigWLevel}):\nn\tgph\tspeed\tdir")      
        # sig wind levels p gph u v
        for j in range(sigWLevel):
            if isnan(htSigW[j]) and isnan(wsSigW[j]) and isnan(wdSigW[j]):
                continue
            print(f"{j}\t{htSigW[j]:.1f}\t{wsSigW[j]:.1f}\t{wdSigW[j]:.1f}")            


        # maximum wind levels
        prMaxW = nc.variables["prMaxW"][i].filled(fill_value=np.nan)
        wsMaxW = nc.variables["wsMaxW"][i].filled(fill_value=np.nan)
        wdMaxW = nc.variables["wdMaxW"][i].filled(fill_value=np.nan)

        print(f"\nstation {stn}\nmax wind levels ({mWndNum}):\nn\tpress\tspeed\tdir")
        for j in range(mWndNum):
            if isnan(prMaxW[j]) and isnan(wsMaxW[j]) and isnan(wdMaxW[j]):
                continue
            print(f"{j}\t{prMaxW[j]:.1f}\t{wsMaxW[j]:.1f}\t{wdMaxW[j]:.1f}")                    


if __name__ == "__main__":
    import argparse
    import json
    parser = argparse.ArgumentParser(
        description="extract ascent from netCDF file",
        add_help=True,
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("-I", "--station-ids", action="store_true", default=False)
    parser.add_argument(
        "-s", "--station",
        action="store",
        default=None,
        help="station name to extract"
    )
    parser.add_argument("files", nargs="*")
    args = parser.parse_args()

    for filename in args.files:
        with open(filename, "rb") as f:
            dump_netcdf(f, args.station, args.station_ids)
            
