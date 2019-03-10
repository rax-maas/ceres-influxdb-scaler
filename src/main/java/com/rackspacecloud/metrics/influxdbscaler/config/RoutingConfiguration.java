package com.rackspacecloud.metrics.influxdbscaler.config;

import com.rackspacecloud.metrics.influxdbscaler.repositories.RoutingInformationRepository;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.RedisStandaloneConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.repository.configuration.EnableRedisRepositories;
import org.springframework.scheduling.annotation.EnableScheduling;

@Configuration
@EnableScheduling
@EnableRedisRepositories(value = "org.springframework.data.redis")
public class RoutingConfiguration {
    @Value("${redis.hostname}")
    private String hostName;

    @Value("${redis.port}")
    private int port;

    @Bean
    RedisStandaloneConfiguration redisStandaloneConfiguration(){
        return new RedisStandaloneConfiguration(hostName, port);
    }

//    @Bean
//    RoutingInformationRepository routingInformationRepository() {
//        return new Routing
//    }

    @Bean
    LettuceConnectionFactory redisConnectionFactory(){
        return new LettuceConnectionFactory(redisStandaloneConfiguration());
    }
}
