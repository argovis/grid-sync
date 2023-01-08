import pandas as pd
import numpy as np
import scipy.io, datetime
import xarray as xr
from pymongo import MongoClient
import util.helpers as h

client = MongoClient('mongodb://database/argo')
db = client.argo
basins = xr.open_dataset('parameters/basinmask_01.nc')

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
meta['_id'] = 'ohc_kg'
meta['data_type'] = 'ocean_heat_content'
meta['data_keys'] = ['ohc_kg']
meta['units'] = ['J/m^2']
meta['date_updated_argovis'] = datetime.datetime.now()
meta['source'] = [{
	'source': ['Kuusela_Giglio2022'],
	'doi': '10.5281/zenodo.6131625',
	'url': 'https://doi.org/10.5281/zenodo.6131625'
}]
meta['levels'] = [15] # really anywhere from 15-300

# write metadata to grid metadata collection
try:
	db['gridMetax'].insert_one(meta)
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
				"metadata": ["ohc_kg"],
				"geolocation": {"type":"Point", "coordinates":[h.tidylon(lon),lat]},
				"basin": h.find_basin(basins, h.tidylon(lon), lat),
				"timestamp": datetime.datetime.utcfromtimestamp(ts),
				"data": [bfr.loc[dict(LONGITUDE=lon, LATITUDE=lat, TIME=t)].data]
			}
			data['_id'] = data['timestamp'].strftime('%Y%m%d%H%M%S') + '_' + str(h.tidylon(lon)) + '_' + str(lat)

			data['data_keys'] = ['kg21_ohc15to300']
			data['units'] = ['J/m^2']

			# nothing to record, drop it
			if np.isnan(data['data']).all():
				continue 

			# mongo doesn't like numpy types, only want 6 decimal places, and grid data is packed as [[grid 1's levels], [grid 2's levels, ...]]:
			data['data'] = [[round(float(x),6) for x in data['data']]]

			# check and see if this lat/long/timestamp lattice point already exists
			record = db['grid_1_1_0.5_0.5'].find_one(data['_id'])
			if record:
				# append and replace
				record['metadata'] = record['metadata'] + data['metadata']
				record['data_keys'] = record['data_keys'] + data['data_keys']
				record['data'] = record['data'] + data['data']
				record['units'] = record['units'] + data['units']

				try:
					db['grid_1_1_0.5_0.5'].replace_one({'_id': data['_id']}, record)
				except BaseException as err:
					print('error: db write replace failure')
					print(err)
					print(data)
			else:
				# insert new record
				try:
					db['grid_1_1_0.5_0.5'].insert_one(data)
				except BaseException as err:
					print('error: db write insert failure')
					print(err)
					print(data)