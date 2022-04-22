import pandas as pd
import numpy as np
import scipy.io, datetime
import xarray as xr
from pymongo import MongoClient

client = MongoClient('mongodb://database/argo')
db = client.argo

# extract data from .mat to xarray, compliments Jacopo
mat=scipy.io.loadmat('data/fullFieldSpaceTrendPchipPotTempGCOS_0015_0300_5_20_10_tseries_global_Blanca.mat')
lon = np.arange(start=20.5, stop=380.5, step=1)
lat = np.arange(start=-64.5, stop=65.5, step=1)
time = pd.date_range("2005-01-15", periods=192, freq = '1M')
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
meta = {}
meta['_id'] = 'OHC'
meta['units'] = 'J/m^2'
meta['levels'] = [15] # really anywhere from 15-300
meta['date_added'] = datetime.datetime.now()
# write metadata to grid metadata collection
try:
	db['grids-meta'].insert_one(meta)
except BaseException as err:
	print('error: db write failure')
	print(err)
	print(meta)

# construct data records
timesteps = list(bfr['TIME'].data)
latpoints = list(bfr['LATITUDE'].data)
lonpoints = list(bfr['LONGITUDE'].data)

for t in timesteps:
	for lat in latpoints:
		for lon in lonpoints:
			data = {
				"g": {"type":"Point", "coordinates":[float(lon),float(lat)]},
				"t": t,
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