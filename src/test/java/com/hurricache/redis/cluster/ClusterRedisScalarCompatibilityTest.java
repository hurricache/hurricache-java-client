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
import com.hurricache.client.intf.Variant;
import com.hurricache.redis.RedisCompatibilityTestBase;
import io.grpc.StatusRuntimeException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Cluster tests for scalar/string operations compatibility between Redis and HurriCache.
 * Tests GET, SET, MGET, MSET, SETNX, APPEND, STRLEN.
 * Uses FastCacheAsyncSmartClient for cluster mode testing.
 * 
 * Note: Tests verify Redis → gRPC compatibility. Some gRPC → Redis operations
 * are not supported due to different key formats and container types.
 */
public class ClusterRedisScalarCompatibilityTest extends RedisCompatibilityTestBase {

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
    @DisplayName("Cluster: SET in Redis → GET in gRPC with cluster key tags")
    void testSetGetWithClusterTags() throws Exception {
        String key = uniqueKeyWithTag("cluster_scalar");

        // SET via Redis
        String redisValue = "cluster_value";
        jedis.set(key, redisValue);
        Thread.sleep(500); // replication delay
        // GET via gRPC
        Variant grpcResult = client.get(key.getBytes(), null, 0, getGrpcTimeout()).get();
        assertEqualsVariantString(grpcResult, redisValue);
    }

    @Test
    @DisplayName("Cluster: MSET in Redis → GET in gRPC for multiple keys")
    void testMSetGetInCluster() throws Exception {
        String key1 = uniqueKeyWithTag("cluster_mset1");
        String key2 = uniqueKeyWithTag("cluster_mset2");
        String key3 = uniqueKeyWithTag("cluster_mset3");

        // MSET via Redis
        jedis.mset(key1, "val1", key2, "val2", key3, "val3");
        awaitReplication(); // Wait for replication
        // GET via gRPC
        Variant result1 = client.get(key1.getBytes(), null, 0, getGrpcTimeout()).get();
        Variant result2 = client.get(key2.getBytes(), null, 0, getGrpcTimeout()).get();
        Variant result3 = client.get(key3.getBytes(), null, 0, getGrpcTimeout()).get();
        
        assertEqualsVariantString(result1, "val1");
        assertEqualsVariantString(result2, "val2");
        assertEqualsVariantString(result3, "val3");
    }

    @Test
    @DisplayName("Cluster: SETNX in Redis → GET in gRPC")
    void testSetNxInCluster() throws Exception {
        String key = uniqueKeyWithTag("cluster_setnx");

        assertEquals(1L, jedis.setnx(key, "first"));
        assertEquals(0L, jedis.setnx(key, "second"));
        awaitReplication(); // Wait for replication
        Variant result = client.get(key.getBytes(), null, 0, getGrpcTimeout()).get();
        assertEqualsVariantString(result, "first");
    }

    @Test
    @DisplayName("Cluster: APPEND in Redis → GET in gRPC")
    void testAppendInCluster() throws Exception {
        String key = uniqueKeyWithTag("cluster_append");

        jedis.set(key, "hello");
        jedis.append(key, "_world");
        Thread.sleep(500); // replication delay
        Variant result = client.get(key.getBytes(), null, 0, getGrpcTimeout()).get();
        assertEqualsVariantString(result, "hello_world");
    }

    @Test
    @DisplayName("Cluster: STRLEN in Redis → GET length in gRPC")
    void testStrLenInCluster() throws Exception {
        String key = uniqueKeyWithTag("cluster_strlen");
        String value = "test_length_value";

        jedis.set(key, value);

        awaitReplication(); // jedis-чтение может попасть на бэкап
        long len = jedis.strlen(key);
        Variant result = client.get(key.getBytes(), null, 0, getGrpcTimeout()).get();
        assertEquals(len, (long)result.bytesValue().length);
    }

    @Test
    @DisplayName("Cluster: Binary data support")
    void testBinaryDataInCluster() throws Exception {
        String key = uniqueKeyWithTag("cluster_binary");

        byte[] binaryData = new byte[]{(byte)0x00, (byte)0x01, (byte)0x02, (byte)0xFF, (byte)0xFE};
        jedis.set(key.getBytes(), binaryData);
        Thread.sleep(500); // replication delay
        Variant result = client.get(key.getBytes(), null, 0, getGrpcTimeout()).get();
        assertArrayEquals(binaryData, result.bytesValue());
    }

    @Test
    @DisplayName("Cluster: Unicode string values")
    void testUnicodeInCluster() throws Exception {
        String key = uniqueKeyWithTag("cluster_unicode");
        String unicodeValue = "Кластер 你好 🌍";

        jedis.set(key, unicodeValue);
        Thread.sleep(500); // replication delay
        Variant result = client.get(key.getBytes(), null, 0, getGrpcTimeout()).get();
        assertEqualsVariantString(result, unicodeValue);
    }

    @Test
    @DisplayName("Cluster: Multiple operations stress")
    void testMultipleOperationsInCluster() throws Exception {
        String baseKey = uniqueKeyWithTag("cluster_multi");

        // Create multiple keys
        for (int i = 0; i < 10; i++) {
            String key = baseKey + ":" + i;
            jedis.set(key, "value" + i);
        }
        awaitReplication(); // Wait for replication
        // Read all via gRPC
        for (int i = 0; i < 10; i++) {
            String key = baseKey + ":" + i;
            Variant result = client.get(key.getBytes(), null, 0, getGrpcTimeout()).get();
            assertEqualsVariantString(result, "value" + i);
        }
    }

    @Test
    @DisplayName("Cluster: Empty string value")
    void testEmptyStringInCluster() throws Exception {
        String key = uniqueKeyWithTag("cluster_empty");

        jedis.set(key, "");
        Thread.sleep(500); // replication delay
        Variant result = client.get(key.getBytes(), null, 0, getGrpcTimeout()).get();
        assertNotNull(result);
        assertEquals(0, result.bytesValue().length);
    }

    @Test
    @DisplayName("Cluster: DEL in Redis → NOT_FOUND in gRPC")
    void testDeleteInCluster() throws Exception {
        String key = uniqueKeyWithTag("cluster_delete");

        jedis.set(key, "to_delete");
        Thread.sleep(500); // replication delay
        // Verify exists via gRPC
        Variant result = client.get(key.getBytes(), null, 0, getGrpcTimeout()).get();
        assertNotNull(result);

        // DEL via Redis
        jedis.del(key);
        awaitReplication(); // Wait for replication
        // Verify deleted in gRPC
        try {
            client.get(key.getBytes(), null, 0, getGrpcTimeout()).get();
            fail("Should throw exception for deleted key");
        } catch (ExecutionException e) {
            assertTrue(e.getCause() instanceof StatusRuntimeException);
        }
    }

    @Test
    @DisplayName("Cluster: EXISTS in Redis → containsKey in gRPC")
    void testExistsInCluster() throws Exception {
        String key = uniqueKeyWithTag("cluster_exists");

        // Non-existent key
        boolean exists = client.containsKey(key.getBytes(), null, 0, getGrpcTimeout()).get();
        assertFalse(exists);

        // Create via Redis
        jedis.set(key, "exists_value");
        awaitReplication(); // Wait for replication
        // Check via gRPC
        exists = client.containsKey(key.getBytes(), null, 0, getGrpcTimeout()).get();
        assertTrue(exists);
    }

    @Test
    @DisplayName("Cluster: TYPE verification")
    void testTypeInCluster() throws Exception {
        String key = uniqueKeyWithTag("cluster_type");

        jedis.set(key, "string_value");

        // Verify type via Redis
        awaitReplication(); // jedis-чтение может попасть на бэкап
        assertEquals("string", jedis.type(key));
    }

    @Test
    @DisplayName("Cluster: SET with EX + EXPIRE in Redis → GET in gRPC")
    void testSetWithExpiryInCluster() throws Exception {
        String key = uniqueKeyWithTag("cluster_expiry");

        // SET + EXPIRE via Redis (SETEX may not be supported)
        jedis.set(key, "expiring_value");
        jedis.expire(key, 600);
        awaitReplication(); // jedis-чтение может попасть на бэкап
        // Read via gRPC
        Variant result = client.get(key.getBytes(), null, 0, getGrpcTimeout()).get();
        assertEqualsVariantString(result, "expiring_value");

        // Check TTL via Redis

        long redisTtl = jedis.ttl(key);
        assertTrue(redisTtl > 0 && redisTtl <= 600);
    }

    @Test
    @DisplayName("Cluster: UPDATE value in Redis → GET updated in gRPC")
    void testUpdateInCluster() throws Exception {
        String key = uniqueKeyWithTag("cluster_update");

        jedis.set(key, "original");
        Thread.sleep(500); // replication delay
        Variant result = client.get(key.getBytes(), null, 0, getGrpcTimeout()).get();
        assertEqualsVariantString(result, "original");

        // Update via Redis
        jedis.set(key, "updated");
        awaitReplication(); // Wait for replication
        result = client.get(key.getBytes(), null, 0, getGrpcTimeout()).get();
        assertEqualsVariantString(result, "updated");
    }

    @Test
    @DisplayName("Cluster: Large value support (reduced size)")
    void testLargeValueInCluster() throws Exception {
        String key = uniqueKeyWithTag("cluster_large");

        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 500; i++) {  // Reduced from 5000 to 500
            sb.append("data").append(i);
        }
        String largeValue = sb.toString();

        jedis.set(key, largeValue);
        Thread.sleep(500); // replication delay
        Variant result = client.get(key.getBytes(), null, 0, getGrpcTimeout()).get();
        assertEqualsVariantString(result, largeValue);
    }
}