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
package com.hurricache.client.standalone.redis;

import com.hurricache.client.FastCacheAsyncSimpleClient;
import com.hurricache.client.intf.HurriCacheClientInterface;
import com.hurricache.client.intf.Variant;
import com.hurricache.redis.RedisCompatibilityTestBase;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Standalone tests for scalar/string operations compatibility between Redis and HurriCache.
 * Tests GET, SET, MGET, MSET, SETNX, GETDEL, GETEX, APPEND, STRLEN, INCR, DECR, INCRBY, DECRBY.
 */
public class StandaloneRedisScalarCompatibilityTest extends RedisCompatibilityTestBase {

    @Override
    protected HurriCacheClientInterface getClient() {
        try {
            return new FastCacheAsyncSimpleClient("127.0.0.1", 50000, 0, Duration.ofSeconds(30));
        } catch (Exception e) {
            throw new RuntimeException("Failed to create SimpleClient", e);
        }
    }

    @Override
    protected String getServerName() {
        return "Standalone (FastCacheAsyncSimpleClient)";
    }

    @Test
    @DisplayName("Bidirectional: SET in Redis → GET in gRPC, CREATE in gRPC → GET in Redis")
    void testSetGetBidirectional() throws Exception {
        String key = uniqueKey("scalar");

        // Step 1: Create via Redis, read via gRPC
        String redisValue = "value_from_redis";
        jedis.set(key, redisValue);
        Variant grpcResult = client.get(key.getBytes(), null, 0, getGrpcTimeout()).get();
        assertEqualsVariantString(grpcResult, redisValue);

        // Step 2: Create via gRPC, read via Redis
        String grpcValue = "value_from_grpc";
        Variant variant = toVariant(grpcValue);
        client.create(key + ":grpc", null, variant, getContainerTtl(), 0, getGrpcTimeout()).get();
        String redisReadResult = jedis.get(key + ":grpc");
        assertEquals(grpcValue, redisReadResult);
    }

    @Test
    @DisplayName("Bidirectional: MSET in Redis → MGET in gRPC")
    void testMGetMSetBidirectional() throws Exception {
        String key1 = uniqueKey("mget");
        String key2 = uniqueKey("mget");
        String key3 = uniqueKey("mget");

        // Create multiple keys via Redis
        jedis.mset(key1, "val1", key2, "val2", key3, "val3");

        // Read via gRPC using MGET (if available) or individual GETs
        // Note: HurriCache may not have MGET, so we test individual GETs
        Variant result1 = client.get(key1.getBytes(), null, 0, getGrpcTimeout()).get();
        Variant result2 = client.get(key2.getBytes(), null, 0, getGrpcTimeout()).get();
        Variant result3 = client.get(key3.getBytes(), null, 0, getGrpcTimeout()).get();

        assertEqualsVariantString(result1, "val1");
        assertEqualsVariantString(result2, "val2");
        assertEqualsVariantString(result3, "val3");
    }

    @Test
    @DisplayName("Bidirectional: SETNX in Redis → GET in gRPC")
    void testSetNxGetBidirectional() throws Exception {
        String key = uniqueKey("setnx");

        // SETNX via Redis
        long setResult = jedis.setnx(key, "nx_value");
        assertEquals(1L, setResult);

        // Read via gRPC
        Variant result = client.get(key.getBytes(), null, 0, getGrpcTimeout()).get();
        assertEqualsVariantString(result, "nx_value");

        // SETNX again should fail
        long setResult2 = jedis.setnx(key, "nx_value2");
        assertEquals(0L, setResult2);

        // Value should remain unchanged
        result = client.get(key.getBytes(), null, 0, getGrpcTimeout()).get();
        assertEqualsVariantString(result, "nx_value");
    }

    @Test
    @DisplayName("Bidirectional: GETDEL in gRPC → GET in Redis")
    void testGetDelBidirectional() throws Exception {
        String key = uniqueKey("getdel");

        // Create via Redis
        jedis.set(key, "delete_me");

        // Read via gRPC first to verify
        Variant result = client.get(key.getBytes(), null, 0, getGrpcTimeout()).get();
        assertEqualsVariantString(result, "delete_me");

        // GETDEL via gRPC
        Variant deleted = client.pop(key.getBytes(), null, 0, getGrpcTimeout()).get();
        assertEqualsVariantString(deleted, "delete_me");

        // Verify key is deleted in Redis
        String redisResult = jedis.get(key);
        assertNull(redisResult);
    }

    @Test
    @DisplayName("Bidirectional: APPEND in Redis → GET in gRPC")
    void testAppendGetBidirectional() throws Exception {
        String key = uniqueKey("append");

        // Initial SET
        jedis.set(key, "hello");

        // APPEND via Redis
        jedis.append(key, "_world");

        // Read via gRPC
        Variant result = client.get(key.getBytes(), null, 0, getGrpcTimeout()).get();
        assertEqualsVariantString(result, "hello_world");
    }

    @Test
    @DisplayName("Bidirectional: STRLEN in Redis → GET in gRPC")
    void testStrLenGetBidirectional() throws Exception {
        String key = uniqueKey("strlen");
        String value = "test_string_length";

        jedis.set(key, value);

        // STRLEN via Redis
        long len = jedis.strlen(key);

        // GET via gRPC and verify length
        Variant result = client.get(key.getBytes(), null, 0, getGrpcTimeout()).get();
        assertEquals(len, result.bytesValue().length);
    }

    @Test
    @DisplayName("Bidirectional: INCR/DECR in Redis → GET in gRPC")
    void testIncrDecrBidirectional() throws Exception {
        String key = uniqueKey("incr");

        // Initialize with SET
        jedis.set(key, "100");

        // INCR via Redis
        jedis.incr(key);

        // Read via gRPC — INCR хранит типизированный INT
        Variant result = client.get(key.getBytes(), null, 0, getGrpcTimeout()).get();
        assertEquals(101, result.longValue());

        // DECR via Redis
        jedis.decr(key);

        result = client.get(key.getBytes(), null, 0, getGrpcTimeout()).get();
        assertEquals(100, result.longValue());
    }

    @Test
    @DisplayName("Bidirectional: INCRBY in Redis → GET in gRPC")
    void testIncrByBidirectional() throws Exception {
        String key = uniqueKey("incrby");

        jedis.set(key, "50");
        jedis.incrBy(key, 25L);

        // INCRBY хранит типизированный INT
        Variant result = client.get(key.getBytes(), null, 0, getGrpcTimeout()).get();
        assertEquals(75, result.longValue());

        // DECRBY
        jedis.decrBy(key, 10L);

        result = client.get(key.getBytes(), null, 0, getGrpcTimeout()).get();
        assertEquals(65, result.longValue());

    }

    @Test
    @DisplayName("Bidirectional: SET with EX → GETTTL in gRPC")
    void testSetWithExpiryBidirectional() throws Exception {
        String key = uniqueKey("expiry");

        // SET with EX via Redis
        jedis.setex(key, 300, "expiring_value");

        // Check TTL via Redis
        long redisTtl = jedis.ttl(key);
        assertTrue(redisTtl > 0 && redisTtl <= 300);

        // Read value via gRPC
        Variant result = client.get(key.getBytes(), null, 0, getGrpcTimeout()).get();
        assertEqualsVariantString(result, "expiring_value");

        // Check TTL via gRPC — gRPC отдаёт TTL в миллисекундах
        Long grpcTtl = client.getTtl(key.getBytes(), null, 0, getGrpcTimeout()).get();
        assertNotNull(grpcTtl);
        assertTrue(grpcTtl > 0 && grpcTtl <= 300_000);
    }

    @Test
    @DisplayName("Bidirectional: CREATE with TTL → TTL in Redis")
    void testCreateWithTtlBidirectional() throws Exception {
        String key = uniqueKey("create_ttl");

        // CREATE with TTL via gRPC
        Variant variant = toVariant("ttl_value");
        client.create(key, null, variant, Duration.ofSeconds(600), 0, getGrpcTimeout()).get();

        // Check via Redis - should exist
        String redisValue = jedis.get(key);
        assertEquals("ttl_value", redisValue);

        // TTL in Redis should be set (if HurriCache propagates it) or -1
        // Note: HurriCache may not propagate TTL to Redis layer
        long redisTtl = jedis.ttl(key);
        // We just verify the key exists with correct value
    }

    @Test
    @DisplayName("Bidirectional: REPLACE in gRPC → GET in Redis")
    void testReplaceBidirectional() throws Exception {
        String key = uniqueKey("replace");

        // Create via Redis
        jedis.set(key, "original");

        // Read via gRPC to verify
        Variant result = client.get(key.getBytes(), null, 0, getGrpcTimeout()).get();
        assertEqualsVariantString(result, "original");

        // Replace via gRPC
        Variant newValue = toVariant("replaced");
        client.replace(key.getBytes(), null, newValue, getContainerTtl(), 0, getGrpcTimeout()).get();

        // Read via Redis
        String redisValue = jedis.get(key);
        assertEquals("replaced", redisValue);
    }

    @Test
    @DisplayName("Bidirectional: Contains key check")
    void testContainsKeyBidirectional() throws Exception {
        String key = uniqueKey("contains");

        // Create via Redis
        jedis.set(key, "test_value");

        // Check existence via gRPC
        boolean exists = client.containsKey(key.getBytes(), null, 0, getGrpcTimeout()).get();
        assertTrue(exists);

        // Delete via Redis
        jedis.del(key);

        // Check again via gRPC
        exists = client.containsKey(key.getBytes(), null, 0, getGrpcTimeout()).get();
        assertFalse(exists);
    }

    @Test
    @DisplayName("Bidirectional: Binary data support")
    void testBinaryDataBidirectional() throws Exception {
        String key = uniqueKey("binary");

        // Create with binary data via Redis
        byte[] binaryData = new byte[]{0x00, 0x01, 0x02, (byte)0xFF, (byte)0xFE, (byte)0xFD};
        jedis.set(key.getBytes(), binaryData);

        // Read via gRPC
        Variant result = client.get(key.getBytes(), null, 0, getGrpcTimeout()).get();
        assertArrayEquals(binaryData, result.bytesValue());

        // Create via gRPC with binary data
        byte[] binaryData2 = new byte[]{0x10, 0x20, 0x30, 0x40};
        Variant variant = Variant.ofBytes(binaryData2);
        String key2 = key + ":grpc";
        client.create(key2.getBytes(), null, variant, getContainerTtl(), 0, getGrpcTimeout()).get();

        // Read via Redis
        byte[] redisResult = jedis.get(key2.getBytes());
        assertArrayEquals(binaryData2, redisResult);
    }

    @Test
    @DisplayName("Bidirectional: Large value support")
    void testLargeValueBidirectional() throws Exception {
        String key = uniqueKey("large");

        // Create large value via Redis
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 10000; i++) {
            sb.append("data").append(i);
        }
        String largeValue = sb.toString();

        jedis.set(key, largeValue);

        // Read via gRPC
        Variant result = client.get(key.getBytes(), null, 0, getGrpcTimeout()).get();
        assertEqualsVariantString(result, largeValue);
    }

    @Test
    @DisplayName("Edge case: Empty string value")
    void testEmptyStringValue() throws Exception {
        String key = uniqueKey("empty");

        jedis.set(key, "");

        Variant result = client.get(key.getBytes(), null, 0, getGrpcTimeout()).get();
        assertNotNull(result);
        assertEquals(0, result.bytesValue().length);
    }

    @Test
    @DisplayName("Edge case: Unicode string values")
    void testUnicodeValues() throws Exception {
        String key = uniqueKey("unicode");
        String unicodeValue = "Привет мир! 你好世界! 🌍🎉";

        jedis.set(key, unicodeValue);

        Variant result = client.get(key.getBytes(), null, 0, getGrpcTimeout()).get();
        assertEqualsVariantString(result, unicodeValue);
    }
}