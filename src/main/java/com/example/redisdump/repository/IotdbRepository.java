package com.example.redisdump.repository;

import com.example.redisdump.dto.TelemetryDTO;
import org.springframework.context.annotation.Bean;

public interface IotdbRepository {
    boolean insertTelemetry(TelemetryDTO dto);
}