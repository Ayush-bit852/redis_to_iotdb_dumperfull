package com.example.redisdump.config;

import com.example.redisdump.entity.IotDbProperties;
import jakarta.annotation.PreDestroy;
import lombok.RequiredArgsConstructor;
import org.apache.iotdb.session.pool.SessionPool;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@RequiredArgsConstructor
public class IotDbSessionPoolConfig {

    private final IotDbProperties iotDbProperties;
    private SessionPool sessionPool;

    @Bean
    public SessionPool sessionPool() {
        this.sessionPool = new SessionPool.Builder()
                .nodeUrls(iotDbProperties.getNodeUrls())
                .user(iotDbProperties.getUsername())
                .password(iotDbProperties.getPassword())
                .maxSize(iotDbProperties.getMaxSize())
                .build();
        return this.sessionPool;
    }

    @PreDestroy
    public void closeSessionPool() {
        if (sessionPool != null) {
            sessionPool.close();
        }
    }

}
