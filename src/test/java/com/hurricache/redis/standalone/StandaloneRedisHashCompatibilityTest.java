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
package com.hurricache.redis.standalone;

import com.hurricache.client.FastCacheAsyncSimpleClient;
import com.hurricache.client.intf.HurriCacheClientInterface;
import com.hurricache.client.intf.Payload;
import com.hurricache.client.intf.Variant;
import com.hurricache.redis.RedisCompatibilityTestBase;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Standalone tests for hash map operations compatibility between Redis and HurriCache.
 * Tests HSET, HGET, HMSET, HMGET, HDEL, HEXISTS, HGETALL, HKEYS, HVALS, HLEN, HINCRBY.
 */
public class StandaloneRedisHashCompatibilityTest extends RedisCompatibilityTestBase {

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
    @DisplayName("Bidirectional: HSET in Redis → HGETALL in gRPC, CREATE MAP in gRPC → HGETALL in Redis")
    void testHSetHGetBidirectional() throws Exception {
        String key = uniqueKey("hash");

        // HSET via Redis
        jedis.hset(key, "field1", "value1");
        jedis.hset(key, "field2", "value2");
        jedis.hset(key, "field3", "value3");

        // Read via gRPC using getMap
        Map<Payload, Variant> grpcMap = client.getMap(key.getBytes(), null, 0, getGrpcTimeout()).get();
        assertEquals(3, grpcMap.size());
        assertEquals("value1", new String(grpcMap.get(p("field1")).bytesValue()));
        assertEquals("value2", new String(grpcMap.get(p("field2")).bytesValue()));
        assertEquals("value3", new String(grpcMap.get(p("field3")).bytesValue()));

        // CREATE MAP via gRPC
        Map<Payload, Variant> grpcData = new LinkedHashMap<>();
        grpcData.put(p("gfield1"), toVariant("gvalue1"));
        grpcData.put(p("gfield2"), toVariant("gvalue2"));
        String grpcKey = key + ":grpc";
        client.createMap(grpcKey.getBytes(), null, grpcData, getContainerTtl(), 0, getGrpcTimeout()).get();

        // Read via Redis using HGETALL
        Map<String, String> redisMap = jedis.hgetAll(grpcKey);
        assertEquals(2, redisMap.size());
        assertEquals("gvalue1", redisMap.get("gfield1"));
        assertEquals("gvalue2", redisMap.get("gfield2"));
    }

    @Test
    @DisplayName("Bidirectional: HMSET in Redis → HGETALL in gRPC")
    void testHmSetHmGetBidirectional() throws Exception {
        String key = uniqueKey("hmset");

        // HMSET via Redis
        Map<String, String> hashFields = new HashMap<>();
        hashFields.put("k1", "v1");
        hashFields.put("k2", "v2");
        hashFields.put("k3", "v3");
        jedis.hmset(key, hashFields);

        // Read via gRPC
        Map<Payload, Variant> grpcMap = client.getMap(key.getBytes(), null, 0, getGrpcTimeout()).get();
        assertEquals(3, grpcMap.size());
        assertEquals("v1", new String(grpcMap.get(p("k1")).bytesValue()));
        assertEquals("v2", new String(grpcMap.get(p("k2")).bytesValue()));
        assertEquals("v3", new String(grpcMap.get(p("k3")).bytesValue()));
    }

    @Test
    @DisplayName("Bidirectional: HDEL in Redis → HGETALL in gRPC")
    void testHDelBidirectional() throws Exception {
        String key = uniqueKey("hdel");

        // Create via Redis
        jedis.hset(key, "field1", "value1");
        jedis.hset(key, "field2", "value2");
        jedis.hset(key, "field3", "value3");

        // HDEL via Redis
        jedis.hdel(key, "field2");

        // Read via gRPC
        Map<Payload, Variant> grpcMap = client.getMap(key.getBytes(), null, 0, getGrpcTimeout()).get();
        assertEquals(2, grpcMap.size());
        assertNotNull(grpcMap.get(p("field1")));
        assertNull(grpcMap.get(p("field2")));
        assertNotNull(grpcMap.get(p("field3")));
    }

    @Test
    @DisplayName("Bidirectional: HSET + HEXISTS in Redis → getInContainer in gRPC")
    void testHExistsBidirectional() throws Exception {
        String key = uniqueKey("hexists");

        // HSET via Redis
        jedis.hset(key, "testfield", "testvalue");

        // HEXISTS via Redis
        assertTrue(jedis.hexists(key, "testfield"));
        assertFalse(jedis.hexists(key, "nonexistent"));

        // Check via gRPC using containsKey for container
        boolean exists = client.containsKey(key.getBytes(), null, p("testfield").getValue(), 0, getGrpcTimeout()).get();
        assertTrue(exists);

        boolean notExists = client.containsKey(key.getBytes(), null, p("nonexistent").getValue(), 0, getGrpcTimeout()).get();
        assertFalse(notExists);
    }

    @Test
    @DisplayName("Bidirectional: HGETALL in Redis → getMap in gRPC")
    void testHGetAllBidirectional() throws Exception {
        String key = uniqueKey("hgetall");

        // Create hash via Redis
        Map<String, String> hashData = new HashMap<>();
        hashData.put("name", "John");
        hashData.put("age", "30");
        hashData.put("city", "NYC");
        jedis.hmset(key, hashData);

        // Read via gRPC
        Map<Payload, Variant> grpcMap = client.getMap(key.getBytes(), null, 0, getGrpcTimeout()).get();
        assertEquals(3, grpcMap.size());
        assertEquals("John", new String(grpcMap.get(p("name")).bytesValue()));
        assertEquals("30", new String(grpcMap.get(p("age")).bytesValue()));
        assertEquals("NYC", new String(grpcMap.get(p("city")).bytesValue()));
    }

    @Test
    @DisplayName("Bidirectional: HKEYS in Redis → getMap keys in gRPC")
    void testHKeysBidirectional() throws Exception {
        String key = uniqueKey("hkeys");

        // Create via Redis
        jedis.hset(key, "alpha", "a");
        jedis.hset(key, "beta", "b");
        jedis.hset(key, "gamma", "c");

        // HKEYS via Redis
        Set<String> redisKeys = jedis.hkeys(key);
        assertEquals(3, redisKeys.size());

        // Read via gRPC and verify keys
        Map<Payload, Variant> grpcMap = client.getMap(key.getBytes(), null, 0, getGrpcTimeout()).get();
        assertEquals(3, grpcMap.size());
        assertTrue(grpcMap.containsKey(p("alpha")));
        assertTrue(grpcMap.containsKey(p("beta")));
        assertTrue(grpcMap.containsKey(p("gamma")));
    }

    @Test
    @DisplayName("Bidirectional: HVALS in Redis → getMap values in gRPC")
    void testHValsBidirectional() throws Exception {
        String key = uniqueKey("hvals");

        // Create via Redis
        jedis.hset(key, "f1", "v1");
        jedis.hset(key, "f2", "v2");

        // HVALS via Redis
        List<String> redisVals = jedis.hvals(key);
        assertEquals(2, redisVals.size());

        // Read via gRPC and verify values
        Map<Payload, Variant> grpcMap = client.getMap(key.getBytes(), null, 0, getGrpcTimeout()).get();
        List<String> grpcVals = new ArrayList<>();
        for (Variant v : grpcMap.values()) {
            grpcVals.add(new String(v.bytesValue()));
        }
        assertEquals(2, grpcVals.size());
    }

    @Test
    @DisplayName("Bidirectional: HLEN in Redis → getSize in gRPC")
    void testHLenBidirectional() throws Exception {
        String key = uniqueKey("hlen");

        // Create via Redis
        jedis.hset(key, "f1", "v1");
        jedis.hset(key, "f2", "v2");
        jedis.hset(key, "f3", "v3");

        // HLEN via Redis
        long redisLen = jedis.hlen(key);
        assertEquals(3L, redisLen);

        // getSize via gRPC
        int grpcLen = client.getSize(key.getBytes(), null, 0, getGrpcTimeout()).get();
        assertEquals(3, grpcLen);
    }

    @Test
    @DisplayName("Bidirectional: HINCRBY in Redis → HGET in gRPC")
    void testHIncrByBidirectional() throws Exception {
        String key = uniqueKey("hincr");

        // HSET initial value
        jedis.hset(key, "counter", "10");

        // HINCRBY via Redis
        jedis.hincrBy(key, "counter", 5);

        // Read via gRPC — после HINCRBY поле типизированное INT
        Map<Payload, Variant> grpcMap = client.getMap(key.getBytes(), null, 0, getGrpcTimeout()).get();
        assertEquals(15, grpcMap.get(p("counter")).longValue());

        // HINCRBY negative
        jedis.hincrBy(key, "counter", -3);

        grpcMap = client.getMap(key.getBytes(), null, 0, getGrpcTimeout()).get();
        assertEquals(12, grpcMap.get(p("counter")).longValue());
    }

    @Test
    @DisplayName("Bidirectional: HSET with binary fields")
    void testHashBinaryFields() throws Exception {
        String key = uniqueKey("hashbin");

        // Binary field names and values
        byte[] fieldBytes = new byte[]{0x01, 0x02, 0x03};
        byte[] valueBytes = new byte[]{0x10, 0x20, 0x30};

        jedis.hset(key.getBytes(), fieldBytes, valueBytes);

        // Read via gRPC
        Map<Payload, Variant> grpcMap = client.getMap(key.getBytes(), null, 0, getGrpcTimeout()).get();
        assertEquals(1, grpcMap.size());
        Payload expectedKey = Payload.of(fieldBytes);
        assertTrue(grpcMap.containsKey(expectedKey));
        assertArrayEquals(valueBytes, grpcMap.get(expectedKey).bytesValue());
    }

    @Test
    @DisplayName("Bidirectional: Update hash field")
    void testHashFieldUpdate() throws Exception {
        String key = uniqueKey("hashupd");

        // Create via Redis
        jedis.hset(key, "field", "original");

        // Update via Redis
        jedis.hset(key, "field", "updated");

        // Read via gRPC
        Map<Payload, Variant> grpcMap = client.getMap(key.getBytes(), null, 0, getGrpcTimeout()).get();
        assertEquals("updated", new String(grpcMap.get(p("field")).bytesValue()));
    }

    @Test
    @DisplayName("Bidirectional: Delete hash container")
    void testHashContainerDelete() throws Exception {
        String key = uniqueKey("hashdel");

        // Create via Redis
        jedis.hset(key, "f1", "v1");

        // Read via gRPC to verify
        Map<Payload, Variant> grpcMap = client.getMap(key.getBytes(), null, 0, getGrpcTimeout()).get();
        assertNotNull(grpcMap);

        // Delete via gRPC
        client.remove(key.getBytes(), null, 0, getGrpcTimeout()).get();

        // Verify in Redis
        assertNull(jedis.hget(key, "f1"));
    }

    @Test
    @DisplayName("Bidirectional: Empty hash")
    void testEmptyHash() throws Exception {
        String key = uniqueKey("hashe empty");

        // Create empty hash via gRPC
        Map<Payload, Variant> emptyMap = new LinkedHashMap<>();
        client.createMap(key.getBytes(), null, emptyMap, getContainerTtl(), 0, getGrpcTimeout()).get();

        // Check in Redis
        long len = jedis.hlen(key);
        assertEquals(0, len);
    }

    @Test
    @DisplayName("Bidirectional: Large hash with many fields")
    void testLargeHash() throws Exception {
        String key = uniqueKey("hashlarge");

        // Create large hash via Redis
        Map<String, String> largeHash = new HashMap<>();
        for (int i = 0; i < 1000; i++) {
            largeHash.put("field" + i, "value" + i);
        }
        jedis.hmset(key, largeHash);

        // Read via gRPC
        Map<Payload, Variant> grpcMap = client.getMap(key.getBytes(), null, 0, getGrpcTimeout()).get();
        assertEquals(1000, grpcMap.size());
    }

    @Test
    @DisplayName("Bidirectional: Hash with null/non-existent key")
    void testHashNonExistent() throws Exception {
        String key = uniqueKey("hashnull");

        // Try to get non-existent hash via gRPC
        try {
            client.getMap(key.getBytes(), null, 0, getGrpcTimeout()).get();
            fail("Should throw exception for non-existent hash");
        } catch (ExecutionException e) {
            // Expected - key not found
            assertTrue(e.getCause() instanceof io.grpc.StatusRuntimeException);
        }
    }

    @Test
    @DisplayName("Bidirectional: HINCRBY creates typed INT field, visible via gRPC")
    void testHIncrByTypedFieldBidirectional() throws Exception {
        String key = uniqueKey("hincrby");

        // HINCRBY по несуществующему полю создаёт его со значением дельты (Redis-семантика)
        long v1 = jedis.hincrBy(key, "counter", 5);
        assertEquals(5L, v1);

        // gRPC видит типизированное INT-поле
        Variant grpcVal = client.getInContainer(key.getBytes(), null, "counter".getBytes(StandardCharsets.UTF_8), 0, getGrpcTimeout()).get();
        assertEquals(5, grpcVal.longValue());

        // Повторный инкремент по типизированному полю
        long v2 = jedis.hincrBy(key, "counter", 7);
        assertEquals(12L, v2);
        assertEquals("12", jedis.hget(key, "counter"));

        grpcVal = client.getInContainer(key.getBytes(), null, "counter".getBytes(StandardCharsets.UTF_8), 0, getGrpcTimeout()).get();
        assertEquals(12, grpcVal.longValue());
    }

    @Test
    @DisplayName("Bidirectional: HINCRBY on legacy string field converts to typed INT")
    void testHIncrByLegacyStringField() throws Exception {
        String key = uniqueKey("hincrby_legacy");

        // Поле создано как строка через HSET
        jedis.hset(key, "counter", "10");

        long v = jedis.hincrBy(key, "counter", 5);
        assertEquals(15L, v);

        // После инкремента поле типизированное — читается как число через gRPC
        Variant grpcVal = client.getInContainer(key.getBytes(), null, "counter".getBytes(StandardCharsets.UTF_8), 0, getGrpcTimeout()).get();
        assertEquals(15, grpcVal.longValue());
    }

    @Test
    @DisplayName("Bidirectional: HINCRBYFLOAT creates typed DOUBLE field, visible via gRPC")
    void testHIncrByFloatBidirectional() throws Exception {
        String key = uniqueKey("hincrbyfloat");

        double v1 = jedis.hincrByFloat(key, "score", 1.5);
        assertEquals(1.5, v1, 0.0001);

        Variant grpcVal = client.getInContainer(key.getBytes(), null, "score".getBytes(StandardCharsets.UTF_8), 0, getGrpcTimeout()).get();
        assertEquals(1.5, grpcVal.doubleValue(), 0.0001);

        double v2 = jedis.hincrByFloat(key, "score", 2.25);
        assertEquals(3.75, v2, 0.0001);
    }

    @Test
    @DisplayName("Bidirectional: HINCRBYFLOAT on INT field converts to DOUBLE")
    void testHIncrByFloatOnIntField() throws Exception {
        String key = uniqueKey("hincrbyfloat_int");

        jedis.hincrBy(key, "score", 10);

        double v = jedis.hincrByFloat(key, "score", 0.5);
        assertEquals(10.5, v, 0.0001);

        Variant grpcVal = client.getInContainer(key.getBytes(), null, "score".getBytes(StandardCharsets.UTF_8), 0, getGrpcTimeout()).get();
        assertEquals(10.5, grpcVal.doubleValue(), 0.0001);
    }

    @Test
    @DisplayName("HINCRBY overflow - error, value unchanged")
    void testHIncrByOverflow() throws Exception {
        String key = uniqueKey("hincrby_overflow");

        jedis.hincrBy(key, "counter", Long.MAX_VALUE);

        assertThrows(redis.clients.jedis.exceptions.JedisDataException.class,
                     () -> jedis.hincrBy(key, "counter", 1));

        assertEquals(String.valueOf(Long.MAX_VALUE), jedis.hget(key, "counter"));
    }

    @Test
    @DisplayName("HINCRBY on non-integer field - error")
    void testHIncrByNonInteger() throws Exception {
        String key = uniqueKey("hincrby_str");

        jedis.hset(key, "name", "hello");

        assertThrows(redis.clients.jedis.exceptions.JedisDataException.class,
                     () -> jedis.hincrBy(key, "name", 1));

        assertEquals("hello", jedis.hget(key, "name"));
    }

    private Payload p(String value) {
        return Payload.of(value.getBytes(StandardCharsets.UTF_8));
    }
}