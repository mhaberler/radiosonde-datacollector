"""
Skew-T Analysis
===============

Classic skew-T/log-p plot using data the radiosonde.mah.priv.at archive.

This example retrieves the temp for Vienna/Hohe Warte (Austria) of
2021-2-21 12:00Z and creates a MetPy skew-T plot with temperature,
dewpoint, and wind barbs.
"""

from datetime import datetime

import matplotlib.pyplot as plt
from metpy.plots import SkewT
from metpy.units import pandas_dataframe_to_unit_arrays, units
import numpy as np

from radiosonde import Station
from pprint import pprint

######################################################################
# Set time using a datetime object and station as variables
#

dt = datetime(2021, 2, 22, 00)

# use WMo id or exact name from station_list.json:
#station = "11035"
station = 'Wien/Hohe Warte'
dt = datetime(2021, 2, 24, 3)

station = "11240"
st = Station(station)

print(f"available ascents for station {station}:")
for syn_time, src, fn in st.available():
    print(syn_time, src, fn)

# 'data' is a pandas.DataFrame with a Metpy units dictionary attached:
data, metadata = st.as_dataframe(date=dt, asUnitArray=True)

# 'metadata' is additional information about the ascent and the data source:
print(f"ascent metadata for station {station} date {dt}:")
pprint(metadata)

######################################################################
# Isolate variables and attach units
#

# Isolate united arrays from dictionary to individual variables
p = data['pressure']
T = data['temperature']
Td = data['dewpoint']
u = data['u_wind']
v = data['v_wind']


######################################################################
# Make Skew-T Plot
# ----------------
#
# The code below makes a basic skew-T plot using the MetPy plot module
# that contains a SkewT class.
#

# Change default to be better for skew-T
fig = plt.figure(figsize=(9, 11))

# Initiate the skew-T plot type from MetPy class loaded earlier
skew = SkewT(fig, rotation=45)

# Plot the data using normal plotting functions, in this case using
# log scaling in Y, as dictated by the typical meteorological plot
skew.plot(p, T, 'r')
skew.plot(p, Td, 'g')
skew.plot_barbs(p[::3], u[::3], v[::3], y_clip_radius=0.03)

# Set some appropriate axes limits for x and y
skew.ax.set_xlim(-30, 40)
skew.ax.set_ylim(1020, 100)

# Add the relevant special lines to plot throughout the figure
skew.plot_dry_adiabats(t0=np.arange(233, 533, 10) * units.K,
                       alpha=0.25, color='orangered')
skew.plot_moist_adiabats(t0=np.arange(233, 400, 5) * units.K,
                         alpha=0.25, color='tab:green')

# does not work for me, unclear why:
# skew.plot_mixing_lines(p=np.arange(1000, 99, -20) * units.hPa,
#                         linestyle='dotted', color='tab:blue')
# this does:
skew.plot_mixing_lines(linestyle='dotted', color='tab:blue')

# Add some descriptive titles
plt.title('{} Sounding'.format(station), loc='left')
plt.title('Valid Time: {}'.format(dt), loc='right')

plt.show()
