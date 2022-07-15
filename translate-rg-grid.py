# usage: python translate-rg-grid.py <temp or psal> <anom or total> <start of file, format YYYYMM> <filesystem path to raw upstream delta file, or 'base' for the 2004-2018 base files> <original url of raw upstream file>
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
data_collection = ''

meta = {}
if var=='temp':
	data_collection = 'temperature_rg'
	meta['_id'] = 'temperature_rg' + metadata_suffix
	meta['data_type'] = 'temperature'
	meta['data_keys'] = ['temperature_rg']
	meta['units'] = ['degree celcius (ITS-90)']
elif var=='psal':
	data_collection = 'salinity_rg'
	meta['_id'] = 'salinity_rg' + metadata_suffix
	meta['data_type'] = 'salinity'
	meta['data_keys'] = ['salinity_rg']
	meta['units'] = ['psu']
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
meta['lonrange'] = [min(lonpoints), max(lonpoints)]
meta['latrange'] = [min(latpoints), max(latpoints)]
meta['timerange'] = [min(dates), max(dates)]
meta['loncell'] = 1
meta['latcell'] = 1

# write metadata to grid metadata collection
try:
	db['gridMetax'].insert_one(meta)
except BaseException as err:
	print('error: db write failure')
	print(err)
	print(meta)

# restore lonpoints to non-tidied version so we can use it for xarray indexing
lonpoints = [float(x) for x in list(clim['LONGITUDE'].data)]

# construct data records
for t in timesteps:
	for lat in latpoints:
		for lon in lonpoints:
			data = {
				"metadata": meta['_id'],
				"geolocation": {"type":"Point", "coordinates":[h.tidylon(lon),lat]},
				"basin": h.find_basin(basins, h.tidylon(lon), lat),
				"timestamp": datetime.datetime(year=2004, month=1, day=15) + dateutil.relativedelta.relativedelta(months=math.floor(t))
			}
			data['_id'] = data['timestamp'].strftime('%Y%m%d%H%M%S') + '_' + str(h.tidylon(lon)) + '_' + str(lat)
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

			# mongo doesn't like numpy types, only want 6 decimal places, and standard format requires each level be its own list:
			data['data'] = [[round(float(x),6)] for x in data['data']]

			# write data to appropriate data collection
			try:
				db[data_collection].insert_one(data)
			except BaseException as err:
				print('error: db write failure')
				print(err)
				print(data)