/*
 * HurriCache Java Client - Redis Compatibility Tests
 * Copyright (C) 2024 Sber Bank GCR
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU General Public License for more details.
 */
package com.hurricache.redis;

import com.hurricache.client.intf.HurriCacheClientInterface;
import com.hurricache.client.intf.Variant;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.UUID;

/**
 * Abstract base class for Redis compatibility tests.
 * Provides bidirectional testing between Redis protocol (via Jedis) and HurriCache gRPC client.
 */
public abstract class RedisCompatibilityTestBase {

    protected Jedis jedis;
    protected JedisPool jedisPool;
    protected HurriCacheClientInterface client;

    /**
     * Returns the HurriCache client implementation to test.
     * Must be implemented by subclasses for standalone/cluster modes.
     */
    protected abstract HurriCacheClientInterface getClient();

    /**
     * Returns the server name for test reporting.
     */
    protected abstract String getServerName();

    /**
     * Returns the timeout for gRPC operations.
     */
    protected Duration getGrpcTimeout() {
        return Duration.ofSeconds(30);
    }

    /**
     * Returns the TTL for created containers.
     */
    protected Duration getContainerTtl() {
        return Duration.ofMinutes(5);
    }

    @BeforeEach
    void setUp() {
        // Setup Jedis connection to Redis
        JedisPoolConfig poolConfig = new JedisPoolConfig();
        poolConfig.setMaxTotal(10);
        poolConfig.setMaxIdle(5);
        poolConfig.setMinIdle(2);

        jedisPool = new JedisPool(poolConfig, "localhost", 6379);
        jedis = jedisPool.getResource();

        // Initialize HurriCache client
        client = getClient();

        // Clean up before each test
        try {
            jedis.flushDB();
        } catch (Exception e) {
            System.err.println("Warning: Could not flush Redis: " + e.getMessage());
        }
    }

    @AfterEach
    void tearDown() {
        try {
            if (jedis != null) {
                jedis.close();
            }
        } catch (Exception e) {
            // Ignore
        }

        try {
            if (jedisPool != null) {
                jedisPool.close();
            }
        } catch (Exception e) {
            // Ignore
        }

        try {
            if (client != null) {
                client.shutdown();
            }
        } catch (Exception e) {
            // Ignore
        }
    }

    /**
     * Helper to compare Variant bytes with expected string.
     */
    protected void assertEqualsVariantString(Variant variant, String expected) {
        if (variant == null) {
            org.junit.jupiter.api.Assertions.fail("Variant is null, expected: " + expected);
        }
        String actual = new String(variant.bytesValue(), StandardCharsets.UTF_8);
        org.junit.jupiter.api.Assertions.assertEquals(expected, actual,
            "Mismatch for server: " + getServerName());
    }

    /**
     * Helper to compare Variant numbers with expected value.
     */
    protected void assertEqualsVariantLong(Variant variant, long expected) {
        if (variant == null) {
            org.junit.jupiter.api.Assertions.fail("Variant is null, expected: " + expected);
        }
        long actual = variant.longValue();
        org.junit.jupiter.api.Assertions.assertEquals(expected, actual,
            "Mismatch for server: " + getServerName());
    }

    /**
     * Helper to create a unique key prefix.
     */
    protected String uniqueKey(String prefix) {
        return prefix + ":" + UUID.randomUUID().toString().substring(0, 8);
    }

    /**
     * Helper to create a unique key with cluster tag.
     */
    protected String uniqueKeyWithTag(String prefix) {
        return prefix + ":{" + UUID.randomUUID().toString().substring(0, 8) + "}";
    }

    /**
     * Helper to create Variant from string.
     */
    protected Variant toVariant(String value) {
        return Variant.ofBytes(value.getBytes(StandardCharsets.UTF_8));
    }

    /**
     * Helper to create Variant from long.
     */
    protected Variant toVariant(long value) {
        return Variant.ofLong(value);
    }

    /**
     * Helper to create Variant from double.
     */
    protected Variant toVariant(double value) {
        return Variant.ofDouble(value);
    }
}
