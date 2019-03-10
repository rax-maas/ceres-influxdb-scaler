package com.rackspacecloud.metrics.influxdbscaler.repositories;

import com.rackspacecloud.metrics.influxdbscaler.models.routing.MaxAndMinSeriesInstances;
import org.springframework.data.repository.CrudRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface MaxMinInstancesRepository extends CrudRepository<MaxAndMinSeriesInstances, String> {
}
