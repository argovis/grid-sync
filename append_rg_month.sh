# usage: bash append_rg_month.sh
# will attempt to add whatever the next month is to the RG Argo climatology

json="$(curl -fsS "https://argovis-api.colorado.edu/summary?id=ratelimiter")"
end_date="$(jq -r '.[0].metadata.rg09.endDate' <<<"$json")"
year="$(date -u -d "$end_date +1 month" +%Y)"
month="$(date -u -d "$end_date +1 month" +%m)"

# does the file exist?
url=https://sio-argo.ucsd.edu/pub/www-argo/RG/RG_ArgoClim_${year}${month}_2019.nc.gz
if ! wget --spider --quiet "$url"; then
    echo "❌ URL not found (404 or other error): $url"
    exit 0
fi

# download and unzip new file into place
wget "$url" 
gunzip RG_ArgoClim_${year}${month}_2019.nc.gz
mv RG_ArgoClim_${year}${month}_2019.nc /tmp/rg/.

# run update
python translate-rg-grid.py temp total ${year}${month} /tmp/rg/RG_ArgoClim_${year}${month}_2019.nc https://sio-argo.ucsd.edu/pub/www-argo/RG/RG_ArgoClim_${year}${month}_2019.nc.gz
python translate-rg-grid.py psal total ${year}${month} /tmp/rg/RG_ArgoClim_${year}${month}_2019.nc https://sio-argo.ucsd.edu/pub/www-argo/RG/RG_ArgoClim_${year}${month}_2019.nc.gz

# delete upstream file
rm /tmp/rg/RG_ArgoClim_${1}${2}_2019.nc

# update the time bounds in the rate limiter summary
python update_ratelimiter_summary.py rg09