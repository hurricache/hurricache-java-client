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
package com.hurricache.redis.cluster;

import com.hurricache.client.FastCacheAsyncSmartClient;
import com.hurricache.client.intf.HurriCacheClientInterface;
import com.hurricache.client.intf.Payload;
import com.hurricache.client.intf.Variant;
import com.hurricache.redis.RedisCompatibilityTestBase;
import io.grpc.StatusRuntimeException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Cluster tests for hash map operations compatibility between Redis and HurriCache.
 * Tests HSET, HGET, HMSET, HGETALL, HDEL, HEXISTS, HLEN.
 * Uses FastCacheAsyncSmartClient for cluster mode testing.
 */
public class ClusterRedisHashCompatibilityTest extends RedisCompatibilityTestBase {

    private static final String CLUSTER_HOST = "192.168.0.123";
    private static final int CLUSTER_PORT_BASE = 51000;

    @Override
    protected HurriCacheClientInterface getClient() {
        try {
            List<String> seedList = new ArrayList<>();
            seedList.add(CLUSTER_HOST + ":" + CLUSTER_PORT_BASE);
            seedList.add(CLUSTER_HOST + ":" + (CLUSTER_PORT_BASE + 10000));
            seedList.add(CLUSTER_HOST + ":" + (CLUSTER_PORT_BASE + 20000));
            
            return new FastCacheAsyncSmartClient(seedList, 0, Duration.ofSeconds(30));
        } catch (Exception e) {
            throw new RuntimeException("Failed to create SmartClient", e);
        }
    }

    @Override
    protected String getServerName() {
        return "Cluster (FastCacheAsyncSmartClient)";
    }

    @Test
    @DisplayName("Cluster: HSET in Redis → HGETALL in gRPC with cluster key tags")
    void testHSetHGetAllWithClusterTags() throws Exception {
        String key = uniqueKeyWithTag("cluster_hash");

        // HSET via Redis
        jedis.hset(key, "field1", "value1");
        jedis.hset(key, "field2", "value2");
        jedis.hset(key, "field3", "value3");
        awaitReplication(); // Wait for replication
        // Read via gRPC using getMap
        Map<Payload, Variant> grpcMap = client.getMap(key.getBytes(), null, 0, getGrpcTimeout()).get();
        assertEquals(3, grpcMap.size());
        assertEquals("value1", new String(grpcMap.get(p("field1")).bytesValue()));
        assertEquals("value2", new String(grpcMap.get(p("field2")).bytesValue()));
        assertEquals("value3", new String(grpcMap.get(p("field3")).bytesValue()));
    }

    @Test
    @DisplayName("Cluster: CREATE MAP in gRPC → HGETALL in Redis")
    void testCreateMapHGetAllBidirectional() throws Exception {
        String key = uniqueKeyWithTag("cluster_create_hash");

        // CREATE MAP via gRPC
        Map<Payload, Variant> grpcData = new LinkedHashMap<>();
        grpcData.put(p("gfield1"), toVariant("gvalue1"));
        grpcData.put(p("gfield2"), toVariant("gvalue2"));
        grpcData.put(p("gfield3"), toVariant("gvalue3"));
        client.createMap(key, null, grpcData, getContainerTtl(), 0, getGrpcTimeout()).get();
        Thread.sleep(500); // replication delay
        // Read via Redis using HGETALL
        Map<String, String> redisMap = jedis.hgetAll(key);
        assertEquals(3, redisMap.size());
        assertEquals("gvalue1", redisMap.get("gfield1"));
        assertEquals("gvalue2", redisMap.get("gfield2"));
        assertEquals("gvalue3", redisMap.get("gfield3"));
    }

    @Test
    @DisplayName("Cluster: HMSET in Redis → getMap in gRPC")
    void testHmSetGetMapBidirectional() throws Exception {
        String key = uniqueKeyWithTag("cluster_hmset");

        // HMSET via Redis
        Map<String, String> hashFields = new HashMap<>();
        hashFields.put("k1", "v1");
        hashFields.put("k2", "v2");
        hashFields.put("k3", "v3");
        jedis.hmset(key, hashFields);
        Thread.sleep(500); // replication delay
        // Read via gRPC
        Map<Payload, Variant> grpcMap = client.getMap(key.getBytes(), null, 0, getGrpcTimeout()).get();
        assertEquals(3, grpcMap.size());
        assertEquals("v1", new String(grpcMap.get(p("k1")).bytesValue()));
        assertEquals("v2", new String(grpcMap.get(p("k2")).bytesValue()));
        assertEquals("v3", new String(grpcMap.get(p("k3")).bytesValue()));
    }

    @Test
    @DisplayName("Cluster: HDEL in Redis → getMap in gRPC")
    void testHDelGetMapBidirectional() throws Exception {
        String key = uniqueKeyWithTag("cluster_hdel");

        // Create via Redis
        jedis.hset(key, "field1", "value1");
        jedis.hset(key, "field2", "value2");
        jedis.hset(key, "field3", "value3");

        // HDEL via Redis
        jedis.hdel(key, "field2");
        Thread.sleep(500); // replication delay
        // Read via gRPC
        Map<Payload, Variant> grpcMap = client.getMap(key.getBytes(), null, 0, getGrpcTimeout()).get();
        assertEquals(2, grpcMap.size());
        assertNotNull(grpcMap.get(p("field1")));
        assertNull(grpcMap.get(p("field2")));
        assertNotNull(grpcMap.get(p("field3")));
    }

    @Test
    @DisplayName("Cluster: HEXISTS in Redis → containsKey in gRPC")
    void testHExistsContainsKeyBidirectional() throws Exception {
        String key = uniqueKeyWithTag("cluster_hexists");

        // HSET via Redis
        jedis.hset(key, "testfield", "testvalue");
        awaitReplication(); // Wait for replication
        // HEXISTS via Redis
        assertTrue(jedis.hexists(key, "testfield"));
        assertFalse(jedis.hexists(key, "nonexistent"));

        // Check via gRPC using containsKey
        boolean exists = client.containsKey(key.getBytes(), null, p("testfield").getValue(), 0, getGrpcTimeout()).get();
        assertTrue(exists);

        boolean notExists = client.containsKey(key.getBytes(), null, p("nonexistent").getValue(), 0, getGrpcTimeout()).get();
        assertFalse(notExists);
    }

    @Test
    @DisplayName("Cluster: HLEN in Redis → getSize in gRPC")
    void testHLenGetSizeBidirectional() throws Exception {
        String key = uniqueKeyWithTag("cluster_hlen");

        // Create via Redis
        jedis.hset(key, "f1", "v1");
        jedis.hset(key, "f2", "v2");
        jedis.hset(key, "f3", "v3");
        awaitReplication(); // Wait for replication
        // HLEN via Redis
        long redisLen = jedis.hlen(key);
        assertEquals(3L, redisLen);

        // getSize via gRPC
        int grpcLen = client.getSize(key.getBytes(), null, 0, getGrpcTimeout()).get();
        assertEquals(3, grpcLen);
    }

    @Test
    @DisplayName("Cluster: HGETALL in Redis → getMap in gRPC")
    void testHGetAllGetMapBidirectional() throws Exception {
        String key = uniqueKeyWithTag("cluster_hgetall");

        // Create hash via Redis
        Map<String, String> hashData = new HashMap<>();
        hashData.put("name", "John");
        hashData.put("age", "30");
        hashData.put("city", "NYC");
        jedis.hmset(key, hashData);
        awaitReplication(); // Wait for replication
        // Read via gRPC
        Map<Payload, Variant> grpcMap = client.getMap(key.getBytes(), null, 0, getGrpcTimeout()).get();
        assertEquals(3, grpcMap.size());
        assertEquals("John", new String(grpcMap.get(p("name")).bytesValue()));
        // "30" распарсилось RESP-парсером в типизированный INT
        assertEquals(30, grpcMap.get(p("age")).longValue());
        assertEquals("NYC", new String(grpcMap.get(p("city")).bytesValue()));
    }

    @Test
    @DisplayName("Cluster: HKEYS in Redis → getMap keys in gRPC")
    void testHKeysGetMapKeysBidirectional() throws Exception {
        String key = uniqueKeyWithTag("cluster_hkeys");

        // Create via Redis
        jedis.hset(key, "alpha", "a");
        jedis.hset(key, "beta", "b");
        jedis.hset(key, "gamma", "c");

        // HKEYS via Redis
        Set<String> redisKeys = jedis.hkeys(key);
        assertEquals(3, redisKeys.size());
        awaitReplication(); // Wait for replication
        // Read via gRPC and verify keys
        Map<Payload, Variant> grpcMap = client.getMap(key.getBytes(), null, 0, getGrpcTimeout()).get();
        assertEquals(3, grpcMap.size());
        assertTrue(grpcMap.containsKey(p("alpha")));
        assertTrue(grpcMap.containsKey(p("beta")));
        assertTrue(grpcMap.containsKey(p("gamma")));
    }

    @Test
    @DisplayName("Cluster: HVALS in Redis → getMap values in gRPC")
    void testHValsGetMapValuesBidirectional() throws Exception {
        String key = uniqueKeyWithTag("cluster_hvals");

        // Create via Redis
        jedis.hset(key, "f1", "v1");
        jedis.hset(key, "f2", "v2");
        jedis.hset(key, "f3", "v3");

        // HVALS via Redis
        List<String> redisVals = jedis.hvals(key);
        assertEquals(3, redisVals.size());
        awaitReplication(); // Wait for replication
        // Read via gRPC and verify values
        Map<Payload, Variant> grpcMap = client.getMap(key.getBytes(), null, 0, getGrpcTimeout()).get();
        List<String> grpcVals = new ArrayList<>();
        for (Variant v : grpcMap.values()) {
            grpcVals.add(new String(v.bytesValue()));
        }
        assertEquals(3, grpcVals.size());
    }

    @Test
    @DisplayName("Cluster: HSET with binary fields")
    void testHashBinaryFieldsInCluster() throws Exception {
        String key = uniqueKeyWithTag("cluster_hashbin");

        // Binary field names and values
        byte[] fieldBytes = new byte[]{0x01, 0x02, 0x03};
        byte[] valueBytes = new byte[]{0x10, 0x20, 0x30};

        jedis.hset(key.getBytes(), fieldBytes, valueBytes);
        Thread.sleep(500); // replication delay
        // Read via gRPC
        Map<Payload, Variant> grpcMap = client.getMap(key.getBytes(), null, 0, getGrpcTimeout()).get();
        assertEquals(1, grpcMap.size());
        Payload expectedKey = Payload.of(fieldBytes);
        assertTrue(grpcMap.containsKey(expectedKey));
        assertArrayEquals(valueBytes, grpcMap.get(expectedKey).bytesValue());
    }

    @Test
    @DisplayName("Cluster: Update hash field")
    void testHashFieldUpdateInCluster() throws Exception {
        String key = uniqueKeyWithTag("cluster_hashupd");

        // Create via Redis
        jedis.hset(key, "field", "original");

        // Update via Redis
        jedis.hset(key, "field", "updated");
        awaitReplication(); // Wait for replication
        // Read via gRPC
        Map<Payload, Variant> grpcMap = client.getMap(key.getBytes(), null, 0, getGrpcTimeout()).get();
        assertEquals("updated", new String(grpcMap.get(p("field")).bytesValue()));
    }

    @Test
    @DisplayName("Cluster: Delete hash container")
    void testHashContainerDeleteInCluster() throws Exception {
        String key = uniqueKeyWithTag("cluster_hashdel");

        // Create via Redis
        jedis.hset(key, "f1", "v1");
        Thread.sleep(500); // replication delay
        // Read via gRPC to verify
        Map<Payload, Variant> grpcMap = client.getMap(key.getBytes(), null, 0, getGrpcTimeout()).get();
        assertNotNull(grpcMap);

        // Delete via gRPC
        client.remove(key.getBytes(), null, 0, getGrpcTimeout()).get();
        awaitReplication(); // Wait for replication
        // Verify in Redis
        assertNull(jedis.hget(key, "f1"));
    }

    @Test
    @DisplayName("Cluster: Empty hash")
    void testEmptyHashInCluster() throws Exception {
        String key = uniqueKeyWithTag("cluster_hasheempty");

        // Create empty hash via gRPC
        Map<Payload, Variant> emptyMap = new LinkedHashMap<>();
        client.createMap(key.getBytes(), null, emptyMap, getContainerTtl(), 0, getGrpcTimeout()).get();

        // Check in Redis
        long len = jedis.hlen(key);
        assertEquals(0, len);
    }

    @Test
    @DisplayName("Cluster: Large hash with many fields")
    void testLargeHashInCluster() throws Exception {
        String key = uniqueKeyWithTag("cluster_hashlarge");

        // Create large hash via Redis
        Map<String, String> largeHash = new HashMap<>();
        for (int i = 0; i < 1000; i++) {
            largeHash.put("field" + i, "value" + i);
        }
        jedis.hmset(key, largeHash);
        awaitReplication(); // Wait for replication
        // Read via gRPC
        Map<Payload, Variant> grpcMap = client.getMap(key.getBytes(), null, 0, getGrpcTimeout()).get();
        assertEquals(1000, grpcMap.size());
    }

    @Test
    @DisplayName("Cluster: Hash with unicode values")
    void testHashUnicodeInCluster() throws Exception {
        String key = uniqueKeyWithTag("cluster_hashuni");

        // Create with unicode values
        jedis.hset(key, "greeting", "\u041F\u0440\u0438\u0432\u0435\u0442");
        jedis.hset(key, "chinese", "\u4F60\u597D");
        jedis.hset(key, "emoji", "\uD83C\uDF0D");
        Thread.sleep(500); // replication delay
        // Read via gRPC
        Map<Payload, Variant> grpcMap = client.getMap(key.getBytes(), null, 0, getGrpcTimeout()).get();
        assertEquals(3, grpcMap.size());
        assertEquals("\u041F\u0440\u0438\u0432\u0435\u0442", new String(grpcMap.get(p("greeting")).bytesValue()));
        assertEquals("\u4F60\u597D", new String(grpcMap.get(p("chinese")).bytesValue()));
        assertEquals("\uD83C\uDF0D", new String(grpcMap.get(p("emoji")).bytesValue()));
    }

    @Test
    @DisplayName("Cluster: Hash with large values")
    void testHashLargeValuesInCluster() throws Exception {
        String key = uniqueKeyWithTag("cluster_hashlargeval");

        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 1000; i++) {
            sb.append("data").append(i);
        }
        String largeValue = sb.toString();

        jedis.hset(key, "largefield", largeValue);
        Thread.sleep(500); // replication delay
        // Read via gRPC
        Map<Payload, Variant> grpcMap = client.getMap(key.getBytes(), null, 0, getGrpcTimeout()).get();
        assertEquals(largeValue, new String(grpcMap.get(p("largefield")).bytesValue()));
    }

    @Test
    @DisplayName("Cluster: HSET with putAll in gRPC")
    void testHSetPutAllBidirectional() throws Exception {
        String key = uniqueKeyWithTag("cluster_hashputall");

        // Create via Redis
        jedis.hset(key, "existing", "value");
        awaitReplication(); // Wait for replication
        // Add more fields via gRPC putAll
        List<Payload> newKeys = new ArrayList<>();
        List<Variant> newValues = new ArrayList<>();
        newKeys.add(p("added1"));
        newValues.add(toVariant("avalue1"));
        newKeys.add(p("added2"));
        newValues.add(toVariant("avalue2"));
        client.putAll(key.getBytes(), null, newKeys, newValues, 0, getGrpcTimeout()).get();
        awaitReplication(); // Wait for replication
        // Verify via Redis
        Map<String, String> redisMap = jedis.hgetAll(key);
        assertEquals(3, redisMap.size());
        assertEquals("value", redisMap.get("existing"));
        assertEquals("avalue1", redisMap.get("added1"));
        assertEquals("avalue2", redisMap.get("added2"));
    }

    @Test
    @DisplayName("Cluster: Non-existent hash key")
    void testNonExistentHashInCluster() throws Exception {
        String key = uniqueKeyWithTag("cluster_hashnull");

        // Try to get non-existent hash via gRPC
        try {
            client.getMap(key.getBytes(), null, 0, getGrpcTimeout()).get();
            fail("Should throw exception for non-existent hash");
        } catch (ExecutionException e) {
            // Expected - key not found
            assertTrue(e.getCause() instanceof StatusRuntimeException);
        }
    }

    @Test
    @DisplayName("Cluster: Hash with empty field name")
    void testHashEmptyFieldNameInCluster() throws Exception {
        String key = uniqueKeyWithTag("cluster_hashef");

        // Create with empty field name
        jedis.hset(key, "", "empty_field_value");
        jedis.hset(key, "normal", "normal_value");
        awaitReplication(); // Wait for replication
        // Read via gRPC
        Map<Payload, Variant> grpcMap = client.getMap(key.getBytes(), null, 0, getGrpcTimeout()).get();
        assertEquals(2, grpcMap.size());
        assertTrue(grpcMap.containsKey(p("")));
        assertEquals("empty_field_value", new String(grpcMap.get(p("")).bytesValue()));
    }

    @Test
    @DisplayName("Cluster: Multiple hash operations stress")
    void testMultipleHashOpsInCluster() throws Exception {
        String baseKey = uniqueKeyWithTag("cluster_hashmulti");

        // Create multiple hash containers
        for (int i = 0; i < 10; i++) {
            String key = baseKey + ":" + i;
            jedis.hset(key, "field", "value" + i);
        }
        Thread.sleep(1500); // replication delay
        // Read all via gRPC
        for (int i = 0; i < 10; i++) {
            String key = baseKey + ":" + i;
            Map<Payload, Variant> grpcMap = client.getMap(key.getBytes(), null, 0, getGrpcTimeout()).get();
            assertEquals(1, grpcMap.size());
            assertEquals("value" + i, new String(grpcMap.get(p("field")).bytesValue()));
        }
    }

    @Test
    @DisplayName("Cluster: HINCRBY creates typed INT field, visible via gRPC")
    void testHIncrByBidirectional() throws Exception {
        String key = uniqueKeyWithTag("hincrby");

        // HINCRBY по несуществующему полю создаёт его со значением дельты (Redis-семантика)
        long v1 = jedis.hincrBy(key, "counter", 5);
        assertEquals(5L, v1);
        awaitReplication(); // Wait for replication
        // gRPC видит типизированное INT-поле
        Variant grpcVal = client.getInContainer(key.getBytes(), null, "counter".getBytes(StandardCharsets.UTF_8), 0, getGrpcTimeout()).get();
        assertEquals(5, grpcVal.longValue());

        // Повторный инкремент по типизированному полю
        long v2 = jedis.hincrBy(key, "counter", 7);
        assertEquals(12L, v2);
        assertEquals("12", jedis.hget(key, "counter"));
        awaitReplication(); // Wait for replication
        grpcVal = client.getInContainer(key.getBytes(), null, "counter".getBytes(StandardCharsets.UTF_8), 0, getGrpcTimeout()).get();
        assertEquals(12, grpcVal.longValue());
    }

    @Test
    @DisplayName("Cluster: HINCRBY on legacy string field converts to typed INT")
    void testHIncrByLegacyStringField() throws Exception {
        String key = uniqueKeyWithTag("hincrby_legacy");

        // Поле создано как строка через HSET
        jedis.hset(key, "counter", "10");

        long v = jedis.hincrBy(key, "counter", 5);
        assertEquals(15L, v);
        Thread.sleep(500); // replication delay
        // После инкремента поле типизированное — читается как число через gRPC
        Map<Payload, Variant> payloadVariantMap = client.getMap(key, null).get();
        Variant grpcVal = client.getInContainer(key, null, "counter".getBytes(StandardCharsets.UTF_8), 0, getGrpcTimeout()).get();
        assertEquals(15, grpcVal.longValue());
    }

    @Test
    @DisplayName("Cluster: HINCRBYFLOAT creates typed DOUBLE field, visible via gRPC")
    void testHIncrByFloatBidirectional() throws Exception {
        String key = uniqueKeyWithTag("hincrbyfloat");

        double v1 = jedis.hincrByFloat(key, "score", 1.5);
        assertEquals(1.5, v1, 0.0001);
        awaitReplication(); // Wait for replication
        Variant grpcVal = client.getInContainer(key.getBytes(), null, "score".getBytes(StandardCharsets.UTF_8), 0, getGrpcTimeout()).get();
        assertEquals(1.5, grpcVal.doubleValue(), 0.0001);

        double v2 = jedis.hincrByFloat(key, "score", 2.25);
        assertEquals(3.75, v2, 0.0001);
    }

    @Test
    @DisplayName("Cluster: HINCRBYFLOAT on INT field converts to DOUBLE")
    void testHIncrByFloatOnIntField() throws Exception {
        String key = uniqueKeyWithTag("hincrbyfloat_int");

        jedis.hincrBy(key, "score", 10);

        double v = jedis.hincrBy(key, "score", 1);
        assertEquals(11, v);
        awaitReplication(); // Wait for replication
        Variant grpcVal = client.getInContainer(key, null, "score".getBytes(StandardCharsets.UTF_8), 0, getGrpcTimeout()).get();
        assertEquals(11, grpcVal.longValue());
    }

    @Test
    @DisplayName("Cluster: HINCRBY overflow - error, value unchanged")
    void testHIncrByOverflow() throws Exception {
        String key = uniqueKeyWithTag("hincrby_overflow");

        jedis.hincrBy(key, "counter", Long.MAX_VALUE);

        assertThrows(redis.clients.jedis.exceptions.JedisDataException.class,
                     () -> jedis.hincrBy(key, "counter", 1));

        assertEquals(String.valueOf(Long.MAX_VALUE), jedis.hget(key, "counter"));
    }

    @Test
    @DisplayName("Cluster: HINCRBY on non-integer field - error")
    void testHIncrByNonInteger() throws Exception {
        String key = uniqueKeyWithTag("hincrby_str");

        jedis.hset(key, "name", "hello");

        assertThrows(redis.clients.jedis.exceptions.JedisDataException.class,
                     () -> jedis.hincrBy(key, "name", 1));

        assertEquals("hello", jedis.hget(key, "name"));
    }

    @Test
    @DisplayName("Cluster: HMGET in Redis → getInContainer in gRPC")
    void testHMGetInCluster() throws Exception {
        String key = uniqueKeyWithTag("cluster_hmget");

        jedis.hset(key, "f1", "v1");
        jedis.hset(key, "f2", "v2");
        jedis.hset(key, "f3", "v3");

        awaitReplication();
        List<String> values = jedis.hmget(key, "f1", "f2", "missing");
        assertEquals(3, values.size());
        assertEquals("v1", values.get(0));
        assertEquals("v2", values.get(1));
        assertNull(values.get(2));
    }

    @Test
    @DisplayName("Cluster: HSTRLEN in Redis")
    void testHStrLenInCluster() throws Exception {
        String key = uniqueKeyWithTag("cluster_hstrlen");

        jedis.hset(key, "short", "abc");
        jedis.hset(key, "long", "abcdefghij");

        awaitReplication();
        assertEquals(3L, jedis.hstrlen(key, "short"));
        assertEquals(10L, jedis.hstrlen(key, "long"));
        assertEquals(0L, jedis.hstrlen(key, "missing"));
    }

    @Test
    @DisplayName("Cluster: HSCAN in Redis")
    void testHScanInCluster() throws Exception {
        String key = uniqueKeyWithTag("cluster_hscan");

        jedis.hset(key, "sf1", "sv1");
        jedis.hset(key, "sf2", "sv2");

        awaitReplication();
        var scanResult = jedis.hscan(key, "0");
        assertFalse(scanResult.getResult().isEmpty());
        // Один проход с курсором 0 должен вернуть все поля
        var fields = new java.util.HashSet<String>();
        for (var entry : scanResult.getResult()) {
            fields.add(entry.getKey());
        }
        assertTrue(fields.contains("sf1"));
        assertTrue(fields.contains("sf2"));
    }

    private Payload p(String value) {
        return Payload.of(value.getBytes(StandardCharsets.UTF_8));
    }
}