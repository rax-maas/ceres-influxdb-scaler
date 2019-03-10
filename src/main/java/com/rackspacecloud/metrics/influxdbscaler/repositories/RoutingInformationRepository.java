package com.rackspacecloud.metrics.influxdbscaler.repositories;

import com.rackspacecloud.metrics.influxdbscaler.models.routing.InfluxDBInstance;
import org.springframework.data.repository.CrudRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface RoutingInformationRepository extends CrudRepository<InfluxDBInstance, String> {
}
