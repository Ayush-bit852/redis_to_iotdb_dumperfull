package com.example.redisdump.repository;

import com.example.redisdump.dto.TelemetryDTO;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.pool.SessionPool;
import org.apache.tsfile.enums.TSDataType;
import org.springframework.stereotype.Repository;

import java.util.*;
import java.util.stream.Collectors;

@Repository
@RequiredArgsConstructor
@Slf4j
public class IotdbRepositoryImpl implements IotdbRepository { // ✅ Not abstract

    private final SessionPool sessionPool;

    @Override
    public boolean insertTelemetry(TelemetryDTO dto) {
        return insertTelemetryBatch(Collections.singletonList(dto));
    }

    @Override
    public boolean insertTelemetryBatch(List<TelemetryDTO> dtoList) {
        Map<String, List<TelemetryDTO>> groupedByDevice = dtoList.stream()
                .collect(Collectors.groupingBy(dto -> "root.vehicle.d" + dto.getDeviceId()));

        boolean allSuccess = true;

        for (Map.Entry<String, List<TelemetryDTO>> entry : groupedByDevice.entrySet()) {
            String devicePath = entry.getKey();
            List<TelemetryDTO> telemetryForDevice = entry.getValue();

            List<Long> timestamps = new ArrayList<>();
            List<List<String>> measurementsList = new ArrayList<>();
            List<List<TSDataType>> typesList = new ArrayList<>();
            List<List<Object>> valuesList = new ArrayList<>();

            for (TelemetryDTO dto : telemetryForDevice) {
                timestamps.add(dto.getFixTime());

                List<String> measurements = Arrays.asList(
                        "id", "deviceId", "protocol", "serverTime", "deviceTime", "fixTime",
                        "outdated", "valid", "latitude", "longitude", "altitude", "speed",
                        "course", "address", "accuracy", "network", "packetType", "uniqueId"
                );
                measurementsList.add(measurements);

                List<TSDataType> types = Arrays.asList(
                        TSDataType.INT64,
                        TSDataType.INT64,
                        TSDataType.TEXT,
                        TSDataType.INT64,
                        TSDataType.INT64,
                        TSDataType.INT64,
                        TSDataType.BOOLEAN,
                        TSDataType.BOOLEAN,
                        TSDataType.DOUBLE,
                        TSDataType.DOUBLE,
                        TSDataType.DOUBLE,
                        TSDataType.DOUBLE,
                        TSDataType.DOUBLE,
                        TSDataType.TEXT,
                        TSDataType.DOUBLE,
                        TSDataType.TEXT,
                        TSDataType.TEXT,
                        TSDataType.TEXT
                );
                typesList.add(types);

                List<Object> values = Arrays.asList(
                        dto.getId(),
                        dto.getDeviceId(),
                        dto.getProtocol(),
                        dto.getServerTime(),
                        dto.getDeviceTime(),
                        dto.getFixTime(),
                        dto.isOutdated(),
                        dto.isValid(),
                        dto.getLatitude(),
                        dto.getLongitude(),
                        dto.getAltitude(),
                        dto.getSpeed(),
                        dto.getCourse(),
                        dto.getAddress(),
                        dto.getAccuracy(),
                        dto.getNetwork(),
                        dto.getPacketType(),
                        dto.getUniqueId()
                );
                valuesList.add(values);
            }

            try {
                sessionPool.insertRecordsOfOneDevice(devicePath, timestamps, measurementsList, typesList, valuesList);
            } catch (IoTDBConnectionException | StatementExecutionException e) {
                log.error("Failed to insert telemetry for device: {}", devicePath, e);
                allSuccess = false;
            }
        }

        return allSuccess;
    }
}
