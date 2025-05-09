package com.example.redisdump.dto;

import lombok.Data;

import java.util.Map;

@Data
public class TelemetryDTO {
    private long id;
    private long deviceId;
    private String protocol;
    private long serverTime;
    private long deviceTime;
    private long fixTime;
    private boolean outdated;
    private boolean valid;
    private double latitude;
    private double longitude;
    private double altitude;
    private double speed;
    private double course;
    private String address;
    private double accuracy;
    private String network;
    private String packetType;
    private String uniqueId;
    private Map<String, Object> attributes;
}
