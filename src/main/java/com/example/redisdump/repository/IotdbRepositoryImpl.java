package com.example.redisdump.repository;

import com.example.redisdump.dto.TelemetryDTO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Repository;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

@Repository
public class IotdbRepositoryImpl implements IotdbRepository {

    private static final Logger log = LoggerFactory.getLogger(IotdbRepositoryImpl.class);

    @Value("${iotdb.url}")
    private String url;

    @Value("${iotdb.username}")
    private String username;

    @Value("${iotdb.password}")
    private String password;

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
        try (Connection conn = DriverManager.getConnection(url, username, password);
             PreparedStatement ps = conn.prepareStatement(INSERT_SQL)) {

            long ts = System.currentTimeMillis();
            ps.setLong(1, ts);
            ps.setLong(2, dto.getId());
            ps.setLong(3, dto.getDeviceId());
            ps.setString(4, dto.getProtocol());
            ps.setLong(5, dto.getServerTime());
            ps.setLong(6, dto.getDeviceTime());
            ps.setLong(7, dto.getFixTime());
            ps.setBoolean(8, dto.isOutdated());
            ps.setBoolean(9, dto.isValid());
            ps.setDouble(10, dto.getLatitude());
            ps.setDouble(11, dto.getLongitude());
            ps.setDouble(12, dto.getAltitude());
            ps.setDouble(13, dto.getSpeed());
            ps.setDouble(14, dto.getCourse());
            ps.setString(15, dto.getAddress());
            ps.setDouble(16, dto.getAccuracy());
            ps.setString(17, dto.getNetwork());
            ps.setString(18, dto.getPacketType());
            ps.setString(19, dto.getUniqueId());

            ps.execute();
            return true;
        } catch (Exception e) {
            log.error("Failed to insert telemetry into IoTDB", e);
            return false;
        }
    }
}