import pandas as pd
import numpy as np
import scipy.io, datetime
import xarray as xr
from pymongo import MongoClient
import util.helpers as h

client = MongoClient('mongodb://database/argo')
db = client.argo

# extract data from .mat to xarray, compliments Jacopo
mat=scipy.io.loadmat('/tmp/ohc/fullFieldSpaceTrendPchipPotTempGCOS_0015_0300_5_20_10_tseries_global_Blanca.mat')
lon = np.arange(start=20.5, stop=380.5, step=1)
lat = np.arange(start=-64.5, stop=65.5, step=1)
time = pd.date_range("2005-01-01", periods=192, freq='MS')
time += datetime.timedelta(days=14)
d_GCOS_temp_zint = mat['d_GCOS_temp_zint']
d_GCOS_temp_zint = np.moveaxis(d_GCOS_temp_zint, 2, 0)
d_GCOS_temp_zint = np.moveaxis(d_GCOS_temp_zint, 2, 1)
bfr = xr.DataArray(
        data=d_GCOS_temp_zint,
        dims=["TIME", "LATITUDE", "LONGITUDE"],
        coords=dict(
            TIME=time,
            LATITUDE=(["LATITUDE"], lat),
            LONGITUDE=(["LONGITUDE"], lon),
        ),
        attrs=dict(
            description="Ocean heat content.",
            units="J/m2",
        ),
    )


# construct a metadata record
timesteps = list(bfr['TIME'].data) 
dates = [datetime.datetime.utcfromtimestamp((t - np.datetime64('1970-01-01T00:00:00Z')) / np.timedelta64(1, 's')) for t in timesteps]
latpoints = [float(x) for x in list(bfr['LATITUDE'].data)]
lonpoints = [float(x) for x in list(bfr['LONGITUDE'].data)]
tidylon = [h.tidylon(x) for x in lonpoints]

meta = {}
meta['_id'] = 'ohc'
meta['units'] = 'J/m^2'
meta['levels'] = [15] # really anywhere from 15-300
meta['date_added'] = datetime.datetime.now()
meta['lonrange'] = [min(tidylon), max(tidylon)]
meta['latrange'] = [min(latpoints), max(latpoints)]
meta['timerange'] = [min(dates), max(dates)]
meta['loncell'] = 1
meta['latcell'] = 1

# write metadata to grid metadata collection
try:
	db['grids-meta'].insert_one(meta)
except BaseException as err:
	print('error: db write failure')
	print(err)
	print(meta)

# construct data records
for t in timesteps:
	ts = (t - np.datetime64('1970-01-01T00:00:00Z')) / np.timedelta64(1, 's')
	for lat in latpoints:
		for lon in lonpoints:
			data = {
				"g": {"type":"Point", "coordinates":[h.tidylon(lon),lat]},
				"t": datetime.datetime.utcfromtimestamp(ts),
				"d": [bfr.loc[dict(LONGITUDE=lon, LATITUDE=lat, TIME=t)].data]
			}

			# nothing to record, drop it
			if np.isnan(data['d']).all():
				continue 

			# mongo doesn't like numpy types
			data['d'] = [float(x) for x in data['d']]

			# only keep 6 decimal places
			data['d'] = [i if type(i) is not float else round(i,6) for i in data['d']]

			# write data to grid data collection
			try:
				db[meta['_id']].insert_one(data)
			except BaseException as err:
				print('error: db write failure')
				print(err)
				print(data)