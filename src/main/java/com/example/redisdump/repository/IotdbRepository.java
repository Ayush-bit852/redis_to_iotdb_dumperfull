package com.example.redisdump.repository;

import com.example.redisdump.dto.TelemetryDTO;

public interface IotdbRepository {
    boolean insertTelemetry(TelemetryDTO dto);
}