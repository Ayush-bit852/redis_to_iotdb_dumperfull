package com.example.redisdump.repository;

import com.example.redisdump.dto.TelemetryDTO;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.pool.SessionPool;
import org.apache.tsfile.enums.TSDataType;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Repository;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

@Repository
@RequiredArgsConstructor
@Slf4j
public class IotdbRepositoryImpl implements IotdbRepository {

    private final SessionPool sessionPool;

    /**
     * We assume you have created the timeseries table:
     *   CREATE TIMESERIES root.vehicle.d0.id            INT64
     *   CREATE TIMESERIES root.vehicle.d0.deviceId      INT64
     *   CREATE TIMESERIES root.vehicle.d0.protocol      TEXT
     *   CREATE TIMESERIES root.vehicle.d0.serverTime    INT64
     *   CREATE TIMESERIES root.vehicle.d0.deviceTime    INT64
     *   CREATE TIMESERIES root.vehicle.d0.fixTime       INT64
     *   CREATE TIMESERIES root.vehicle.d0.outdated      BOOLEAN
     *   CREATE TIMESERIES root.vehicle.d0.valid         BOOLEAN
     *   CREATE TIMESERIES root.vehicle.d0.latitude      DOUBLE
     *   CREATE TIMESERIES root.vehicle.d0.longitude     DOUBLE
     *   CREATE TIMESERIES root.vehicle.d0.altitude      DOUBLE
     *   CREATE TIMESERIES root.vehicle.d0.speed         DOUBLE
     *   CREATE TIMESERIES root.vehicle.d0.course        DOUBLE
     *   CREATE TIMESERIES root.vehicle.d0.address       TEXT
     *   CREATE TIMESERIES root.vehicle.d0.accuracy      DOUBLE
     *   CREATE TIMESERIES root.vehicle.d0.network       TEXT
     *   CREATE TIMESERIES root.vehicle.d0.packetType    TEXT
     *   CREATE TIMESERIES root.vehicle.d0.uniqueId      TEXT
     */
    private static final String INSERT_SQL =
            "INSERT INTO root.vehicle.d0(timestamp, id, deviceId, protocol, serverTime, deviceTime, fixTime, \n" +
                    "    outdated, valid, latitude, longitude, altitude, speed, course, \n" +
                    "    address, accuracy, network, packetType, uniqueId)\n" +
                    "VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

    @Override
    public boolean insertTelemetry(TelemetryDTO dto) {
        return insertTelemetryBatch(Collections.singletonList(dto));
    }

    public boolean insertTelemetryBatch(List<TelemetryDTO> dtoList) {
        String deviceId = "root.vehicle.d0";

        List<Long> timestamps = new ArrayList<>();
        List<List<String>> measurementsList = new ArrayList<>();
        List<List<TSDataType>> typesList = new ArrayList<>();
        List<List<Object>> valuesList = new ArrayList<>();

        // Use parallelStream() to process the DTO list concurrently
        dtoList.parallelStream().forEach(dto -> {
            timestamps.add(dto.getFixTime());

            List<String> measurements = Arrays.asList(
                    "id", "deviceId", "protocol", "serverTime", "deviceTime", "fixTime",
                    "outdated", "valid", "latitude", "longitude", "altitude", "speed",
                    "course", "address", "accuracy", "network", "packetType", "uniqueId"
            );
            measurementsList.add(measurements);

            List<TSDataType> types = Arrays.asList(
                    TSDataType.INT64,    // id
                    TSDataType.INT64,    // deviceId
                    TSDataType.TEXT,     // protocol
                    TSDataType.INT64,    // serverTime
                    TSDataType.INT64,    // deviceTime
                    TSDataType.INT64,    // fixTime
                    TSDataType.BOOLEAN,  // outdated
                    TSDataType.BOOLEAN,  // valid
                    TSDataType.DOUBLE,   // latitude
                    TSDataType.DOUBLE,   // longitude
                    TSDataType.DOUBLE,   // altitude
                    TSDataType.DOUBLE,   // speed
                    TSDataType.DOUBLE,   // course
                    TSDataType.TEXT,     // address
                    TSDataType.DOUBLE,   // accuracy
                    TSDataType.TEXT,     // network
                    TSDataType.TEXT,     // packetType
                    TSDataType.TEXT      // uniqueId
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
        });

        try {
            // Insert the collected data into IoTDB
            sessionPool.insertRecordsOfOneDevice(deviceId, timestamps, measurementsList, typesList, valuesList);
            return true;
        } catch (IoTDBConnectionException | StatementExecutionException e) {
            log.error("Failed to insert telemetry batch into IoTDB", e);
            return false;
        }
    }

}
