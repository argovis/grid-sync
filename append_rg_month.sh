# usage: bash append_rg_month.sh YYYY MM

# download and unzip new file into place
wget https://sio-argo.ucsd.edu/pub/www-argo/RG/RG_ArgoClim_${1}${2}_2019.nc.gz
gunzip RG_ArgoClim_${1}${2}_2019.nc.gz
mv RG_ArgoClim_${1}${2}_2019.nc /tmp/rg/.

# run update
python translate-rg-grid.py temp total ${1}${2} /tmp/rg/RG_ArgoClim_${1}${2}_2019.nc https://sio-argo.ucsd.edu/pub/www-argo/RG/RG_ArgoClim_${1}${2}_2019.nc.gz
python translate-rg-grid.py psal total ${1}${2} /tmp/rg/RG_ArgoClim_${1}${2}_2019.nc https://sio-argo.ucsd.edu/pub/www-argo/RG/RG_ArgoClim_${1}${2}_2019.nc.gz

# delete upstream file
rm /tmp/rg/RG_ArgoClim_${1}${2}_2019.nc

# update the time bounds in the rate limiter summary
python update_ratelimiter_summary.py rg09