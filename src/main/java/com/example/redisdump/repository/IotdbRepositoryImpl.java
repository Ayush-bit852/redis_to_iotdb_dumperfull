package com.example.redisdump.repository;

import com.example.redisdump.dto.TelemetryDTO;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.pool.SessionPool;
import org.springframework.stereotype.Repository;

import java.io.Serializable;
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
        String deviceId = "root.vehicle.d0";
        List<String> measurements = Arrays.asList(
                "id", "deviceId", "protocol", "serverTime", "deviceTime", "fixTime",
                "outdated", "valid", "latitude", "longitude", "altitude", "speed",
                "course", "address", "accuracy", "network", "packetType", "uniqueId"
        );

        List<? extends Serializable> values = Arrays.asList(
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

        try {
            sessionPool.insertRecordsOfOneDevice(
                    deviceId,
                    values.parallelStream().collect(each -> each, Collections::list),
                    measurements,
                    values);
            return true;
        } catch (IoTDBConnectionException | StatementExecutionException e) {
            log.error("Failed to insert telemetry into IoTDB", e);
            return false;
        }
    }
}
