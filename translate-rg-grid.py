# usage: python translate-rg-grid.py <temp or psal> <anom or total>
import xarray, sys, datetime, dateutil, math, numpy
from pymongo import MongoClient

var = sys.argv[1]
grid = sys.argv[2]

client = MongoClient('mongodb://database/argo')
db = client.argo

# choose input file and load
if var=='temp':
	input_clim = '/tmp/rg/RG_ArgoClim_Temperature_2019.nc' # see https://sio-argo.ucsd.edu/RG_Climatology.html
elif var=='pasl':
	input_clim = '/tmp/rg/RG_ArgoClim_Salinity_2019.nc' 
clim = xarray.open_dataset(input_clim, decode_times=False)

# construct a metadata record
meta = {}
if var=='temp':
	meta['_id'] = 'rgTemp'
	meta['units'] = 'degree celcius (ITS-90)'
elif var=='psal':
	meta['_id'] = 'rgPsal'
	meta['units'] = 'psu'
if grid=='anom':
	meta['_id'] += 'Anom'
elif grid=='total':
	meta['_id'] += 'Total'

meta['levels'] = list(clim['PRESSURE'].data)
meta['levels'] = [float(x) for x in meta['levels']]
meta['date_added'] = datetime.datetime.now()

# write metadata to grid metadata collection
try:
	db['grids-meta'].insert_one(meta)
except BaseException as err:
	print('error: db write failure')
	print(err)
	print(meta)

# construct data records
timesteps = list(clim['TIME'].data) # months since 2004-01-01T00:00:00Z
latpoints = list(clim['LATITUDE'].data)
lonpoints = list(clim['LONGITUDE'].data)
for t in timesteps:
	for lat in latpoints:
		for lon in lonpoints:
			data = {
				"g": {"type":"Point", "coordinates":[float(lon),float(lat)]},
				"t": datetime.datetime(year=2004, month=1, day=15) + dateutil.relativedelta.relativedelta(months=math.floor(t))
			}
			if var == 'temp' and grid =='anom':
				data['d'] = list(clim['ARGO_TEMPERATURE_ANOMALY'].loc[dict(LONGITUDE=lon, LATITUDE=lat, TIME=t)].data)
			elif var == 'psal' and grid=='anom':
				data['d'] = list(clim['ARGO_SALINITY_ANOMALY'].loc[dict(LONGITUDE=lon, LATITUDE=lat, TIME=t)].data)
			elif var == 'temp' and grid == 'total':
				data['d'] = list(clim['ARGO_TEMPERATURE_ANOMALY'].loc[dict(LONGITUDE=lon, LATITUDE=lat, TIME=t)].data +  clim['ARGO_TEMPERATURE_MEAN'].loc[dict(LONGITUDE=lon, LATITUDE=lat)].data)
			elif var == 'psal' and grid == 'total':
				data['d'] = list(clim['ARGO_SALINITY_ANOMALY'].loc[dict(LONGITUDE=lon, LATITUDE=lat, TIME=t)].data +  clim['ARGO_SALINITY_MEAN'].loc[dict(LONGITUDE=lon, LATITUDE=lat)].data)
			# nothing to record, drop it
			if numpy.isnan(data['d']).all():
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






