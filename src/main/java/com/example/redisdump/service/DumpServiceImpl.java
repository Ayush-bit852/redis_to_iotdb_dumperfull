package com.example.redisdump.service;

import com.example.redisdump.dto.TelemetryDTO;
import com.example.redisdump.repository.IotdbRepository;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.connection.DataType;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;

@Slf4j
@Service
@RequiredArgsConstructor
public class DumpServiceImpl implements DumpService {

    private static final int BATCH_SIZE = 100;
    private static final int MAX_CONSECUTIVE_EMPTIES = 3;
    private static final int MAX_DELETE_ATTEMPTS = 3;

    private final StringRedisTemplate redisTemplate;
    private final IotdbRepository iotdbRepository;
    private final ObjectMapper objectMapper;

    @Override
    @Scheduled(fixedDelayString = "${dump.fixedDelay:60000}")
    public void dumpAll() {
        try {
            processAllListKeys();
        } catch (Exception e) {
            log.error("Critical error in dumpAll processing", e);
        }
    }

    private void processAllListKeys() {
        Set<String> keys = redisTemplate.keys("*");
        if (keys == null || keys.isEmpty()) {
            log.debug("No keys found in Redis");
            return;
        }

        keys.stream()
                .filter(Objects::nonNull)
                .forEach(this::processListKeyWithCleanup);
    }

    private void processListKeyWithCleanup(String key) {
        try {
            if (!isListType(key)) return;

            long processed = processListElements(key);
            if (processed > 0) {
                safelyRemoveEmptyKey(key);
            }
        } catch (Exception e) {
            log.error("Unexpected error processing key '{}'", key, e);
        }
    }

    private long processListElements(String key) {
        int emptyCycles = 0;
        long totalProcessed = 0;

        while (emptyCycles < MAX_CONSECUTIVE_EMPTIES) {
            List<String> batch = safelyGetBatch(key);
            if (batch.isEmpty()) {
                emptyCycles++;
                continue;
            }

            emptyCycles = 0;
            int successful = processBatchWithRetry(key, batch);

            if (successful == batch.size()) {
                totalProcessed += successful;
                trimProcessedElements(key, successful);
                deleteKeyAfterSuccess(key); // Ensure deletion after successful processing
            } else {
                log.warn("Partial processing ({}/{}) for '{}'", successful, batch.size(), key);
                break;
            }
        }
        return totalProcessed;
    }

    private List<String> safelyGetBatch(String key) {
        try {
            return redisTemplate.opsForList().range(key, 0, BATCH_SIZE - 1);
        } catch (Exception e) {
            log.error("Batch retrieval failed for '{}'", key, e);
            return Collections.emptyList();
        }
    }

    private int processBatchWithRetry(String key, List<String> batch) {
        int successCount = 0;
        for (String element : batch) {
            if (processElementWithRetry(key, element)) {
                successCount++;
            }
        }
        return successCount;
    }

    private boolean processElementWithRetry(String key, String element) {
        try {
            TelemetryDTO dto = objectMapper.readValue(element, TelemetryDTO.class);
            return iotdbRepository.insertTelemetry(dto);
        } catch (JsonProcessingException e) {
            log.error("JSON parsing error: {}", e.getOriginalMessage());
            return false;
        } catch (Exception e) {
            log.error("Insertion error", e);
            return false;
        }
    }

    private void trimProcessedElements(String key, int processed) {
        try {
            redisTemplate.opsForList().trim(key, processed, -1);
        } catch (Exception e) {
            log.error("Trim failed for '{}'", key, e);
        }
    }

    private void deleteKeyAfterSuccess(String key) {
        try {
            // If all elements in the list are processed successfully, delete the Redis key.
            redisTemplate.delete(key);
            log.info("Successfully deleted Redis key '{}'", key);
        } catch (Exception e) {
            log.error("Failed to delete Redis key '{}'", key, e);
        }
    }

    private void safelyRemoveEmptyKey(String key) {
        int attempts = 0;
        while (attempts < MAX_DELETE_ATTEMPTS) {
            try {
                Long size = redisTemplate.opsForList().size(key);
                if (size == null || size == 0) {
                    redisTemplate.delete(key);
                    log.info("Successfully removed empty key '{}'", key);
                    return;
                }
                log.debug("Key '{}' not empty (size {}), preserving", key, size);
                return;
            } catch (Exception e) {
                attempts++;
                log.warn("Delete attempt {} failed for '{}'", attempts, key, e);
            }
        }
        log.error("Failed to delete key '{}' after {} attempts", key, MAX_DELETE_ATTEMPTS);
    }

    private boolean isListType(String key) {
        try {
            return DataType.LIST == redisTemplate.type(key);
        } catch (Exception e) {
            log.warn("Type check failed for '{}'", key, e);
            return false;
        }
    }
}
