package com.example.redisdump;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class RedisToIotdbDumperApplication {
    public static void main(String[] args) {
        SpringApplication.run(RedisToIotdbDumperApplication.class, args);
    }
}