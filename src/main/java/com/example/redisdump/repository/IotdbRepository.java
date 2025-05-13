package com.example.redisdump.repository;

import com.example.redisdump.dto.TelemetryDTO;

import java.util.List;

public interface IotdbRepository {
    boolean insertTelemetryBatch(List<TelemetryDTO> dtoList);
    boolean insertTelemetry(TelemetryDTO dto); // ✅ Single insert method
}
