package com.example.redisdump.entity;

import lombok.Data;
import lombok.RequiredArgsConstructor;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
@ConfigurationProperties(prefix = "iotdb")
@Data
@RequiredArgsConstructor
public class IotDbProperties {
    private List<String> nodeUrls;
    private String username;
    private String password;
    private int maxSize;
}
