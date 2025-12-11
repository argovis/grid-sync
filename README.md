## grid translation scripts

This repo contains scripts to translate upstream gridded products from their original format into JSON docs appropriate for Argovis, and insert them into MongoDB. The expectation is that appropriate collections and indexes have already been created via [https://github.com/argovis/db-schema](https://github.com/argovis/db-schema). See usage instructions for each script at the top of the respective `translate-*` files.

### updating RG

New data comes out for RG every month; `append_rg_month.sh` handles the database update and summary updates. An minimal example pod to run this update for the next month not currently written to Argovis is below, where:

 - `argovis/grid-sync:250812` is built from `Dockerfile`
 - The PVC `rg` is pre-populated with the base RG temperature and salinity grids (2000-2019).

```
apiVersion: v1
kind: Pod
metadata:
  name: grid-sync
  labels:
    tier: api
spec:
  volumes:
    - name: rg
      persistentVolumeClaim:
        claimName: rg
  containers:
  - name: schema
    imagePullPolicy: Always
    image: argovis/grid-sync:250812
    #command: ['sleep', '1000000']
    command: ['bash', 'append_rg_month.sh']
    volumeMounts:
      - mountPath: "/tmp/rg"
        name: rg
    resources:
      requests:
        memory: "0Gi"
        cpu: "0m"
      limits:
        memory: 1000Mi
        cpu: 1000m
  restartPolicy: Never
```

### RG from scratch

Consider adding more current months to the following scripts, and:

```
bash rg-temp-total-batch.sh
bash rg-psal-total-batch.sh
python update_ratelimiter_summary.py rg09
```

### localGPspace from scratch

```
python translate-ohc-grid.py
python update_ratelimiter_summary.py localGPspace
```
