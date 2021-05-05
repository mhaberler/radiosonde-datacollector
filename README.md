


# radiosonde-datacollector

## What

radiosonde-datacollector collates [radiosonde](https://en.wikipedia.org/wiki/Radiosonde) [soundings](https://www.meteoswiss.admin.ch/home/measurement-and-forecasting-systems/atmosphere/radio-soundings.html)  from different sources, and converts it into a web-friendly format for further use by applications such as [radiosonde](https://radiosonde.mah.priv.at/app/) and XXXX. Typical use cases include [Skew-T](https://www.weather.gov/source/zhu/ZHU_Training_Page/convective_parameters/skewt/skewtinfo.html)  and [Stuve](http://www.csun.edu/~hmc60533/CSUN_103/weather_exercises/soundings/smog_and_inversions/Understanding%20Stuve_v3.htm) diagrams.

 The goal is to provide global coverage for sounding data with minimum delay and fast retrieval for client apps.

## Try it out:

[radiosonde](https://radiosonde.mah.priv.at/) is a simple Javascript application based on the venerable [bootleaf](https://bootleaf.xyz/) code.

## Screen shot

![shot](https://static.mah.priv.at/public/radiosonde-screenshot.jpg)
## Why

Radiosonde data is very useful for meteo forecasting, in particular for aviation use and for correlating weather forecasts with actually measured data. However, there is no single source and access method for raw data providing global coverage, and there is no single file format for that data. radiosonde-datacollector deals with different sources,  their file formats, and converts them into a single format - a compressed [GeoJSON](https://geojson.org/) file. [Here is an example](https://radiosonde.mah.priv.at/data/fm94/11/035/2021/05/11035_20210501_000000.geojson) from Vienna/Austria. Typical terms used for such files include sounding, "temps" (temperature soundiing), or "ascent" - the latter term being used throughout this code.

## Where does the data come from
Without going into the organisational intricacies of [weather bureaucracies](https://public.wmo.int/en), I found two data aggregators which together provide decent global coverage:

 1. [Deutscher Wetterdienst](https://www.dwd.de/EN/Home/home_node.html) through its [GISC](https://gisc.dwd.de/wisportal/#) service
 2. [NOAA](https://www.noaa.gov/) through its [MADIS](https://madis.ncep.noaa.gov/) service

GISC provides a push service delivering data by mail, http/https put, sftp or ftp for individual files or pre-packaged subscriptions, of which the 'TEMP_Data-global_FM94' is relevant for this purpose. This code currently uses https put as delivery method to an nginx-based server.
MADIS provides [anonymous ftp access](ftp://madis-data.ncep.noaa.gov/point/raob/netcdf/) which can be mirrored. This is what this code does.

## How much
There are some [2600 registered locations](https://radiosonde.mah.priv.at/static/station_list.txt) which provide meteorological data. Ontop, there are  mobile stations like research vessels which provide soundings from varying locations. Of these, currently about 700 provide sounding data. The [radiosonde-datacollector summary file](https://radiosonde.mah.priv.at/data-dev/summary.geojson) currently retains three days of sounding data and that amounts to about 4000 soundings - so, on average, two soundings per day and station.

The data provided by DWD uses the more modern [FM94 BUFR](https://www.romsaf.org/romsaf_bufr.pdf) format which includes the flight path, and very dense samples (like every 2 seconds). The MADIS data is based on the older [FM35 format](http://vietorweather.net/wxp/appendix1/Formats/TEMP.html) wrapped into a [netCDF](https://www.unidata.ucar.edu/software/netcdf/)-formatted file and has no flight path information, Also it has rather course vertical resolution, which varies depending on contribution organisation (sometimes within a country).


## How big
As a rule of thumb, assume 10kB per sounding ([brotli](https://github.com/google/brotli)-compressed geojson). So a year's worth of soundings might be 5GB.

##  Usage from Python

Ths original [Skew-T example](https://unidata.github.io/python-gallery/examples/SkewT_Example.html) uses the UofWyoming data source.

[I've adapted it](https://github.com/mhaberler/radiosonde-datacollector/blob/master/examples/SkewT_Example.py) to use the radiosonde data source:


![SkewT Diagram using radiosonde-datacollector as source](https://raw.githubusercontent.com/mhaberler/radiosonde-datacollector/master/examples/thalerhof.jpg)

## The gory details
Following in next installment.

## Related services
The University of Wyoming runs an archive of soundings with pretty good coverage - [example here](http://weather.uwyo.edu/cgi-bin/bufrraob.py?datetime=2021-02-24%2012:00:00&id=10238&type=TEXT:LIST). This website is also used from [Python code](https://unidata.github.io/python-gallery/examples/SkewT_Example.html).

## Credits
The MADIS processing code is a straight lift from [skewt](https://github.com/johnckealy/skewtapi/blob/master/scripts/query_madis.py) - thanks, John!
