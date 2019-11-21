# ceres-influxdb-scaler
## Purpose
This service is responsible for two different things:
* Collecting metrics on deployed InfluxDB instances using influxdb restful api. it uses `SHOW STATS` query.
* getting InfluxDB instance with minimum series count among all of the InfluxDB instances in the Kubernetes cluster.

**NOTE:** This is a _singleton_ service. Never deploy more than one replica of this service.
