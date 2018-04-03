The application is completely dockerized.

## Requirements
docker

## Start application
```
docker-machine start
bash start_api.sh
```

## Access home endpoint - Should return Welcome to customer representative app!!!
```
http://0.0.0.0:5000
```

## Refresh cache or load cache with data from redshift. Need to run this the first time and whenever needed later
```
http://0.0.0.0:5000/refresh
```

## Get client info
```
http://0.0.0.0:5000/client/ALBALA FAMILY TRUST
http://0.0.0.0:5000/client/<clientName>
```

## Get Property Address info
```
http://0.0.0.0:5000/address/7290 N DEARING AVE FRESNO CA 937200314
http://0.0.0.0:5000/address/<propertyAddress>
```

## Enter postgres command line
```
docker exec -it dockerfiles_targetdb_1 psql -U postgres -d postgres
select * from public.transactions;
```

## Cleanup docker containers
```
bash cleanup.sh
```
