SpaceONE Statistics Service


## run celery beat
```shell script
cd src && spaceone celery spaceone.statistics -m . -c ./spaceone/statistics/conf/worker.yaml 
./spaceone/statistics/conf/beat.yaml # beat config

```

### Beat server config example
```
GLOBAL:
  DATABASES:
    default:
      db: 'test_stat_celery'
      host: 127.0.0.1
      port: 27017
      username: ''
      password: ''
  ENDPOINT:
    celery:
      endpoint: "localhost:50051"
      version: v1
  CACHES:
    default:
      backend: spaceone.core.cache.redis_cache.RedisCache
      host: localhost
      port: 6379
      db: 1
      encoding: utf-8
      socket_timeout: 10
      socket_connect_timeout: 10

  TOKEN: xxx.xxx.xxx # spaceon token
  
  LOG:
    loggers:
      spaceone:
        handlers:
          - console
          - file
      celery: # please set celery logger
        handlers:
          - console
          - file
  CELERY:
    mode: BEAT
    config:
      broker_url: redis://localhost:6379 
    schedules:
      stat_scheduler:
        task: spaceone.statistics.task.stat_hourly_scheduler.stat_hour_scheduler
        rule_type: interval
        rule:
          period: hours
          every: 1

```

## run celery worker
```shell script
cd src && spaceone celery spaceone.statistics -m . -c ./spaceone/statistics/conf/worker.yaml 
./spaceone/statistics/conf/worker.yaml # worker config

```

### worker server config example
```
GLOBAL:
  DATABASES:
    default:
      db: 'test_stat_celery'
      host: 127.0.0.1
      port: 27017
      username: ''
      password: ''
  ENDPOINT:
    celery:
      endpoint: "localhost:50051"
      version: v1
  CACHES:
    default:
      backend: spaceone.core.cache.redis_cache.RedisCache
      host: localhost
      port: 6379
      db: 1
      encoding: utf-8
      socket_timeout: 10
      socket_connect_timeout: 10

  TOKEN: xxx.xxx.xxx # spaceon token
  
  LOG:
    loggers:
      spaceone:
        handlers:
          - console
          - file

  CELERY:
    mode: WORKER
    config:
      broker_url: redis://localhost:6379

```

