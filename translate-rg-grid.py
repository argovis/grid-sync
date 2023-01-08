# usage: python translate-rg-grid.py <temp or psal> <anom or total> <start of file, format YYYYMM> <filesystem path to raw upstream delta file, or 'base' for the 2004-2018 base files> <original url of raw upstream file>
# expects upstream data at /tmp/rg
import xarray, sys, datetime, dateutil, math, numpy
from pymongo import MongoClient
import util.helpers as h

# command line parameters, for legibility
var = sys.argv[1]
grid = sys.argv[2]
startTime = sys.argv[3]
filepath = sys.argv[4]
url = sys.argv[5]

# db connection
client = MongoClient('mongodb://database/argo')
db = client.argo

# data files
basins = xarray.open_dataset('parameters/basinmask_01.nc')
baseTempPath = '/tmp/rg/RG_ArgoClim_Temperature_2019.nc'
baseSalinityPath = '/tmp/rg/RG_ArgoClim_Salinity_2019.nc'
baseClimPath = ''		# ie the 2004-2018 climatology that includes the mean
climPath = ''			# the dataset including the anomoly of interest, could be base or monthly extension files
if var=='temp':
	baseClimPath = baseTempPath
elif var=='psal':
	baseClimPath = baseSalinityPath
baseClim = xarray.open_dataset(baseClimPath, decode_times=False)
if filepath == 'base':
	clim = baseClim
else:
	clim = xarray.open_dataset(filepath, decode_times=False)

# construct a metadata record
timesteps = list(clim['TIME'].data) # months since start of file
dates = [datetime.datetime(year=2004, month=1, day=15) + dateutil.relativedelta.relativedelta(months=math.floor(t)) for t in timesteps]
latpoints = [float(x) for x in list(clim['LATITUDE'].data)]
lonpoints = [float(h.tidylon(x)) for x in list(clim['LONGITUDE'].data)]
metadata_suffix = '_' + str(startTime)

meta = {}
if var=='temp':
	meta['_id'] = 'temperature_rg' + metadata_suffix
	meta['data_type'] = 'temperature'
elif var=='psal':
	meta['_id'] = 'salinity_rg' + metadata_suffix
	meta['data_type'] = 'salinity'
if grid=='anom':
	meta['_id'] += '_Anom'
elif grid=='total':
	meta['_id'] += '_Total'

meta['date_updated_argovis'] = datetime.datetime.now()
meta['source'] = [{
	'source': ['Roemmich-Gilson Argo Climatology'],
	'url': url
}]
meta['levels'] = list(clim['PRESSURE'].data)
meta['levels'] = [float(x) for x in meta['levels']]

# write metadata to grid metadata collection
try:
	db['gridMeta'].insert_one(meta)
except BaseException as err:
	print('error: db write failure')
	print(err)
	print(meta)

# restore lonpoints to non-tidied version so we can use it for xarray indexing
lonpoints = [float(x) for x in list(clim['LONGITUDE'].data)]

for t in timesteps:
	for lat in latpoints:
		for lon in lonpoints:
			# construct single-grid data record
			data = {
				"metadata": [meta['_id']],
				"geolocation": {"type":"Point", "coordinates":[h.tidylon(lon),lat]},
				"basin": h.find_basin(basins, h.tidylon(lon), lat),
				"timestamp": datetime.datetime(year=2004, month=1, day=15) + dateutil.relativedelta.relativedelta(months=math.floor(t))
			}
			
			data['_id'] = data['timestamp'].strftime('%Y%m%d%H%M%S') + '_' + str(h.tidylon(lon)) + '_' + str(lat)

			data['data_keys'] = []
			data['units'] = []
			if var == 'temp':
				data['data_keys'].append('rg09_temperature')
				data['units'].append('degree celcius (ITS-90)')
			elif var == 'psal':
				data['data_keys'].append('rg09_salinity')
				data['units'].append('psu')

			if var == 'temp' and grid =='anom':
				data['data'] = list(clim['ARGO_TEMPERATURE_ANOMALY'].loc[dict(LONGITUDE=lon, LATITUDE=lat, TIME=t)].data)
			elif var == 'psal' and grid=='anom':
				data['data'] = list(clim['ARGO_SALINITY_ANOMALY'].loc[dict(LONGITUDE=lon, LATITUDE=lat, TIME=t)].data)
			elif var == 'temp' and grid == 'total':
				data['data'] = list(clim['ARGO_TEMPERATURE_ANOMALY'].loc[dict(LONGITUDE=lon, LATITUDE=lat, TIME=t)].data + baseClim['ARGO_TEMPERATURE_MEAN'].loc[dict(LONGITUDE=lon, LATITUDE=lat)].data)
			elif var == 'psal' and grid == 'total':
				data['data'] = list(clim['ARGO_SALINITY_ANOMALY'].loc[dict(LONGITUDE=lon, LATITUDE=lat, TIME=t)].data + baseClim['ARGO_SALINITY_MEAN'].loc[dict(LONGITUDE=lon, LATITUDE=lat)].data)
			# nothing to record, drop it
			if numpy.isnan(data['data']).all():
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

			