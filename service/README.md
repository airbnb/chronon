# Chronon Feature Service

The feature service module consists of code to bring up a service that provides a thin shim around the Fetcher code. This 
is meant to aid Chronon adopters who either need a quicker way to get a feature serving layer up and running or need to 
build a way to retrieve features and typically work in a non-JVM based organization. 

## Core Technology

The Chronon Feature Service is built on top of the [Vert.x](https://vertx.io/) JVM framework. Vert.x is a high-performance
web framework which supports HTTP and gRPC based services. 

## Running locally

To build the service sub-module:
```bash
~/workspace/chronon $ sbt "project service" clean assembly
```

To test out the service, you also need to build a concrete instantiation of the [Api](https://github.com/airbnb/chronon/blob/main/online/src/main/scala/ai/chronon/online/Api.scala#L187).
We can leverage the [quickstart Mongo API](https://github.com/airbnb/chronon/tree/main/quickstart/mongo-online-impl) for this:
```bash
~/workspace/chronon $ cd quickstart/mongo-online-impl
~/workspace/chronon/quickstart/mongo-online-impl $ sbt assembly
...
[success] Total time: 1 s, completed Nov 6, 2024, 2:35:26 PM
```
This command will write out a file in the target/scala-2.12 sub-directory.

We can now use this to start up the feature service:
```bash
~/workspace/chronon $ java -jar service/target/scala-2.12/service-vertx_service-*.jar run ai.chronon.service.WebServiceVerticle \
-Dserver.port=9000 -conf service/src/main/resources/example_config.json
...
14:39:26.626 [vert.x-eventloop-thread-1] INFO  a.chronon.service.WebServiceVerticle - HTTP server started on port 9000
14:39:26.627 [vert.x-eventloop-thread-0] INFO  i.v.c.i.l.c.VertxIsolatedDeployer - Succeeded in deploying verticle
```

A few things to call out so you can customize:
- Choose your port (this is where you'll hit your webservice with traffic)
- Update the example_config.json (specifically confirm the path to the mongo-online-impl assembly jar matches your setup)

If you'd like some real data to query from the feature service, make sure to run through the relevant steps of the 
[Quickstart - Online Flows](https://chronon.ai/getting_started/Tutorial.html#online-flows) tutorial. 

Some examples to curl the webservice:
```bash
$ curl 'http://localhost:9000/ping'
$ curl 'http://localhost:9000/config'
$ curl -X POST   'http://localhost:9000/v1/features/join/quickstart%2Ftraining_set.v2'   -H 'Content-Type: application/json'   -d '[{"user_id": "5"}]'
```

## Metrics

The Vert.x feature service relies on the same statsd host / port coordinates as the rest of the Chronon project - 
[Metrics](https://github.com/airbnb/chronon/blob/main/online/src/main/scala/ai/chronon/online/Metrics.scala#L135). When configured correctly,
the service will emit metrics captured by [Vert.x](https://vertx.io/docs/vertx-micrometer-metrics/java/#_http_client), JVM metrics as well as metrics
captured by existing Chronon Fetcher code.

To view these metrics for your locally running feature service:
- Install the [statsd-logger](https://github.com/jimf/statsd-logger) npm module (`npm install -g statsd-logger`)
- Run the command - `statsd-logger`

Now you should see metrics of the format:
```bash
$ statsd-logger 
Server listening on 0.0.0.0:8125
StatsD Metric: jvm.buffer.memory.used 12605920|g|#statistic:value,id:direct
StatsD Metric: jvm.threads.states 0|g|#statistic:value,state:blocked
StatsD Metric: jvm.memory.used 8234008|g|#statistic:value,area:nonheap,id:Compressed Class Space
StatsD Metric: jvm.threads.states 19|g|#statistic:value,state:runnable
StatsD Metric: system.load.average.1m 1.504883|g|#statistic:value
StatsD Metric: vertx.http.server.active.requests 0|g|#statistic:value,method:GET,path:/ping
StatsD Metric: ai.zipline.join.fetch.join_request.count 1|c|#null,null,null,null,environment:join.fetch,owner:quickstart,team:quickstart,production:false,join:quickstart_training_set_v2
StatsD Metric: ai.zipline.join.fetch.group_by_request.count 1|c|#null,null,accuracy:SNAPSHOT,environment:join.fetch,owner:quickstart,team:quickstart,production:false,group_by:quickstart_purchases_v1,join:quickstart_training_set_v2
...
```

## Features Lookup Response Structure

The /v1/features/join and /v1/features/groupby endpoints are bulkGet endpoints (against a single GroupBy or Join). Users can request multiple lookups, for example:
```bash
$ curl -X POST   'http://localhost:9000/v1/features/join/quickstart%2Ftraining_set.v2'   -H 'Content-Type: application/json'   -d '[{"user_id": "5"}, {"user_id": "7"}]'
```

The response status is 4xx (in case of errors parsing the incoming json request payload), 5xx (internal error like the KV store being unreachable) or 200 (some / all successful lookups).
In case of the 200 response, the payload looks like the example shown below:
```json
{
  "results": [
    {
      "status": "Success",
      "entityKeys": {
        "user_id": "5"
      },
      "features": {
        "A": 12,
        "B": 24
      }
    },
    {
      "status": "Success",
      "entityKeys": {
        "user_id": "7"
      },
      "features": {
        "A": 36,
        "B": 48,
      }
    }
  ]
}
```
