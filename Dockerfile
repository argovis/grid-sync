FROM python:3.9

RUN apt-get update -y
RUN apt-get install -y nano rsync
RUN apt-get upgrade -y zlib1g
RUN pip install nose netCDF4 pymongo xarray numpy geopy scipy

WORKDIR /app
COPY translate-rg-grid.py translate-rg-grid.py
COPY translate-ohc-grid.py translate-ohc-grid.py
RUN chown -R 1000660000 /app
