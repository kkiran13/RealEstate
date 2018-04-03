Design 1: The ETL application in `worker.py` is designed as below:

1. Files are sent via FTP to a S3 bucket.
2. A bucket notification is set on this S3 bucket to send events to SQS when a new object is uploaded to the bucket or when an existing object is modified.
3. The body of SQS message contains the S3 keys of new files which are uploaded in S3 bucket
4. A python job is continuously polling the SQS queue to read messages from the queue once every specified time interval
5. A manifest file is created from message bodies read from SQS and uploaded to S3 which is then used to copy data from S3 to redshift staging database.
6. Once the data is in staging table, upsert logic is used to merge data in staging and target table (which is present in public schema)
7. Find distinct primary keys in staging table -> delete records from target table with these primary keys -> insert all records from staging to target table -> truncate staging table

Design 2: A simpler version in `simple_etl.py` is designed as below:
This approach does not contain Redshift copy command as it does not work using docker containers to mock AWS resources
1. File is upload to S3 and SQS message is posted with S3 key.
2. SQS is polled to read message bodies containing S3 keys
3. Message body is parsed and S3 files are download to local machine for processing
4. Downloaded file is inserted row by row into staging table
5. Upsert logic is used to merge staging and target tables
6. SQS message is deleted on successful upsert

## Start ETL application
```
bash start_etl.sh
```

## Access API Home
Returns `Welcome to ETL process tool!!`
```
http://0.0.0.0:5000/
```

## Manually trigger ETL process. This triggers ETL process as described in design 2 above
Returns `ETL Process complete. Check Postgres Database using command: 

`docker exec -it dockerfiles_targetdb_1 psql -U postgres -d postgres -c 'SELECT * FROM public.transactions'
```
http://0.0.0.0:5000/process
```

## Mock S3 bucket
`http://localhost:9444` or `http://mocks3:9444` if `/etc/hosts` has an entry `127.0.0.1 mocks3`

## Mock SQS queue
`http://localhost:9324` or `http://mocksqs:9324` if `/etc/hosts` has an entry `127.0.0.1 mocksqs`

## Mock redshift
Add an entry `127.0.0.1 targetdb` to `/etc/hosts` so that the same connection parameters can be used when using a local installation of postgres and also when using postgres docker container

## Enter postgres command line
```
docker exec -it dockerfiles_targetdb_1 psql -U postgres -d postgres
select * from staging.transactions;
select * from public.transactions;
```

## Cleanup docker containers
```
bash cleanup.sh
```

