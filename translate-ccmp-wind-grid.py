# usage: python translate-ccmp-wind-grid.py <ccmp .nc file>

import pandas as pd
import numpy as np
import scipy.io, datetime, sys, xarray, math
from pymongo import MongoClient
import util.helpers as h

# parameter files, db connections, raw data
basins = xarray.open_dataset('parameters/basinmask_01.nc')

client = MongoClient('mongodb://database/argo')
db = client.argo

ds = xarray.open_dataset(sys.argv[1], decode_times=False)

print(ds['time'])

# data = {}
# metadata = {}
# t0 = datetime.datetime.strptime('1987-01-01T00:00:00Z', '%Y-%m-%dT%H:%M:%SZ')

# t = (t0 + datetime.timedelta(hours=float(ds['time'][0].data)))
# metadata['_id'] = datetime.datetime.strftime(t, '%Y%m%d')
# metadata['data_type'] = 'ccmp-wind'
# metadata['data_info'] = [
# 	['uwnd', 'vwnd', 'ws', 'nobs', 'timestep'],
# 	['units', 'long_name'],
# 	[
# 		[ds['uwnd'].attrs['units'], ds['uwnd'].attrs['long_name']],
# 		[ds['vwnd'].attrs['units'], ds['vwnd'].attrs['long_name']],
# 		[ds['ws'].attrs['units'], ds['ws'].attrs['long_name']],
# 		[None, ds['uwnd'].attrs['long_name']],
# 		[None, 'record timestamp']
# 	]
# ]

# metadata['date_updated_argovis'] = datetime.datetime.now()
# metadata['source'] = [
# 	{
# 		'source': ['CCMP Wind Vector Analysis Product'],
# 		'url': 'https://data.remss.com/ccmp/v03.0/daily/y'+datetime.datetime.strftime(t, '%Y')+'/m'+datetime.datetime.strftime(t, '%m')+'/' + sys.argv[1] 
# 	}
# ]

# try:
# 	db['ccmpMeta'].replace_one({'_id': metadata['_id']}, metadata, upsert=True)
# except BaseException as err:
# 	print('error: db write replace failure')
# 	print(err)
# 	print(metadata)

# for latidx in range(len(ds['latitude'])):
# 	latitude = float(ds['latitude'][latidx].data)
# 	for lonidx in range(len(ds['longitude'])):
# 		longitude = h.tidylon(float(ds['longitude'][lonidx].data))
# 		for timeidx in range(len(ds['time'])):
# 			time = float(ds['time'][timeidx].data)

# 			uwnd = float(ds['uwnd'][latidx][lonidx][timeidx].data)
# 			vwnd = float(ds['vwnd'][latidx][lonidx][timeidx].data)
# 			ws = float(ds['ws'][latidx][lonidx][timeidx].data)
# 			nobs = float(ds['nobs'][latidx][lonidx][timeidx].data)
# 			timestep = t0 + datetime.timedelta(hours=time)

# 			if math.isnan(uwnd) and math.isnan(vwnd) and math.isnan(ws) and math.isnan(nobs):
# 				continue

# 			record = db['ccmp'].find_one({'_id': str(longitude) + '_' + str(latitude)})
# 			if record:
# 				# append to the timeseries
# 				data = record
# 				data['data'][0].append(uwnd)
# 				data['data'][1].append(vwnd)
# 				data['data'][2].append(ws)
# 				data['data'][3].append(nobs)
# 				data['data'][4].append(timestep)
# 			else:
# 				# create a new timeseries
# 				data['_id'] = str(longitude) + '_' + str(latitude)
# 				data['metadata'] = metadata['_id']
# 				data['geolocation'] = {
# 					'type': 'Point',
# 					'coordinates': [longitude, latitude]
# 				}
# 				data['basin'] = h.find_basin(basins, longitude, latitude),
# 				data['basin'] = data['basin'][0] # ?
# 				data['data'] = [[uwnd],[vwnd],[ws],[nobs],[timestep]]

# 			try:
# 				db['ccmp'].replace_one({'_id': data['_id']}, data, upsert=True)
# 			except BaseException as err:
# 				print('error: db write replace failure')
# 				print(err)
# 				print(data)