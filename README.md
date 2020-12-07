SpaceONE Statistics Service


## run celery worker
```shell script
cd src && spaceone celery spaceone.statistics -m . -c ./spaceone/statistics/conf/worker.yaml 
./spaceone/statistics/conf/worker.yaml

```

## run celery beat
```shell script
cd src && spaceone celery spaceone.statistics -m . -c ./spaceone/statistics/conf/worker.yaml 
./spaceone/statistics/conf/beat.yaml

```