
from datetime import datetime
from numpy import arange
import pandas as pd
from scipy.optimize import curve_fit
from matplotlib import pyplot

import matplotlib.pyplot as plt
import numpy as np

from radiosonde import Station
from pprint import pprint


df = pd.DataFrame()

for station in ["11010", "11035", "11120", "11240", "11520", "11747", "11952"]:
    st = Station(station)
    a = st.available()
    print(f"available ascents for station {station}: {len(a)}")
    for syn_time, src, fn in a:
        print(syn_time, src, fn)
        if src == "madis":
            continue
        adf, metadata = st.as_dataframe(date=syn_time, relativeTime=True, relativeElevation=False)
        adf.drop(columns=['pressure', 'gpheight', 'temperature', 'dewpoint',
              'u_wind', 'v_wind',  'latitude', 'longitude'])
        if not df.empty:
            df = pd.concat([df, adf], ignore_index=True)
            print(len(df.index), "samples")
        else:
            df = adf

# https://machinelearningmastery.com/curve-fitting-with-python/

# define the true objective function
def objective(x, a, b, c):
	return a * x + b * x**2 + c

x = df['time']
y = df['elevation']

# curve fit
popt, _ = curve_fit(objective, x, y)
# summarize the parameter values
a, b, c = popt
print('y = %.5f * x + %.5f * x^2 + %.5f' % (a, b, c))
# plot input vs output
pyplot.scatter(x, y)
# define a sequence of inputs between the smallest and largest known inputs
x_line = arange(min(x), max(x), 1)
# calculate the output for the range
y_line = objective(x_line, a, b, c)
# create a line plot for the mapping function
pyplot.plot(x_line, y_line, '--', color='red')
pyplot.show()


# der Zusammenhang 2.Ordnung:  y = 6.00649 * x + -0.00009 * x^2 + 40.89698
