# ceres-influxdb-scaler
## Purpose
This service is responsible for two different things:
* Collecting metrics on deployed InfluxDB instances using influxdb restful api. it uses `SHOW STATS` query.
* getting InfluxDB instance with minimum series count among all of the InfluxDB instances in the Kubernetes cluster.

**NOTE:** This is a _singleton_ service. Never deploy more than one replica of this service.

## Development
* build the application using command: `mvn clean install`
* Run the service using command: `java -jar target/influxdb-scaler-0.0.1-SNAPSHOT.jar`

**NOTE:** If you see some error like this `Caused by: java.lang.IllegalStateException: Unimplemented`. You should run `kubectl get pods` and then re-run the command `java -jar target/influxdb-scaler-0.0.1-SNAPSHOT.jar`


