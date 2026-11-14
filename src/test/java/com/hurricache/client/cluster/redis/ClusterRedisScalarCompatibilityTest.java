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
package com.hurricache.client.cluster.redis;

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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

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
        Thread.sleep(1500); // replication delay
        Variant result = client.get(key, null, 0, getGrpcTimeout()).get();
        assertEqualsVariantString(result, largeValue);
    }

    // =========================================================================
    // ATOMIC SCALARS
    // =========================================================================

    @Test
    @DisplayName("Cluster: INCR/DECR in Redis → typed INT in gRPC")
    void testIncrDecrInCluster() throws Exception {
        String key = uniqueKeyWithTag("cluster_incr");

        jedis.set(key, "100");
        assertEquals(101L, jedis.incr(key));
        assertEquals(100L, jedis.decr(key));

        awaitReplication();
        Variant result = client.get(key.getBytes(), null, 0, getGrpcTimeout()).get();
        assertEquals(100, result.longValue());
    }

    @Test
    @DisplayName("Cluster: INCRBY/DECRBY in Redis → typed INT in gRPC")
    void testIncrByDecrByInCluster() throws Exception {
        String key = uniqueKeyWithTag("cluster_incrby");

        jedis.set(key, "50");
        assertEquals(75L, jedis.incrBy(key, 25L));
        assertEquals(55L, jedis.decrBy(key, 20L));

        awaitReplication();
        Variant result = client.get(key.getBytes(), null, 0, getGrpcTimeout()).get();
        assertEquals(55, result.longValue());
    }

    @Test
    @DisplayName("Cluster: INCRBYFLOAT in Redis → typed DOUBLE in gRPC")
    void testIncrByFloatInCluster() throws Exception {
        String key = uniqueKeyWithTag("cluster_incrbyfloat");

        jedis.set(key, "10.5");
        double v = jedis.incrByFloat(key, 0.25);
        assertEquals(10.75, v, 0.0001);

        awaitReplication();
        Variant result = client.get(key.getBytes(), null, 0, getGrpcTimeout()).get();
        assertEquals(10.75, result.doubleValue(), 0.0001);
    }

    // =========================================================================
    // READ-MODIFY-DELETE
    // =========================================================================

    @Test
    @DisplayName("Cluster: GETDEL in Redis")
    void testGetDelInCluster() throws Exception {
        String key = uniqueKeyWithTag("cluster_getdel");

        jedis.set(key, "to_getdel");
        assertEquals("to_getdel", jedis.getDel(key));

        awaitReplication();
        assertFalse(client.containsKey(key.getBytes(), null, 0, getGrpcTimeout()).get());
    }

    @Test
    @DisplayName("Cluster: GETEX with EX in Redis")
    void testGetExInCluster() throws Exception {
        String key = uniqueKeyWithTag("cluster_getex");

        jedis.set(key, "getex_value");
        String v = jedis.getEx(key, redis.clients.jedis.params.GetExParams.getExParams().ex(300));
        assertEquals("getex_value", v);

        awaitReplication();
        long ttl = jedis.ttl(key);
        assertTrue(ttl > 0 && ttl <= 300);
    }

    @Test
    @DisplayName("Cluster: GETSET in Redis")
    void testGetSetInCluster() throws Exception {
        String key = uniqueKeyWithTag("cluster_getset");

        jedis.set(key, "old_value");
        String old = jedis.getSet(key, "new_value");
        assertEquals("old_value", old);

        awaitReplication();
        Variant result = client.get(key.getBytes(), null, 0, getGrpcTimeout()).get();
        assertEqualsVariantString(result, "new_value");
    }

    // =========================================================================
    // MULTI-KEY
    // =========================================================================

    @Test
    @DisplayName("Cluster: MGET in Redis for multiple keys")
    void testMGetInCluster() throws Exception {
        String key1 = uniqueKeyWithTag("cluster_mget1");
        String key2 = uniqueKeyWithTag("cluster_mget2");

        jedis.mset(key1, "mv1", key2, "mv2");
        awaitReplication();

        List<String> values = jedis.mget(key1, key2);
        assertEquals(2, values.size());
        assertEquals("mv1", values.get(0));
        assertEquals("mv2", values.get(1));
    }

    @Test
    @DisplayName("Cluster: MSETNX in Redis")
    void testMSetNxInCluster() throws Exception {
        String key1 = uniqueKeyWithTag("cluster_msetnx1");
        String key2 = uniqueKeyWithTag("cluster_msetnx2");

        // Все ключи новые — создаются
        assertEquals(1L, jedis.msetnx(key1, "nx1", key2, "nx2"));

        // Хотя бы один существует — ничего не пишется
        assertEquals(0L, jedis.msetnx(key1, "other", uniqueKeyWithTag("cluster_msetnx3"), "nx3"));

        awaitReplication();
        assertEqualsVariantString(client.get(key1.getBytes(), null, 0, getGrpcTimeout()).get(), "nx1");
        assertEqualsVariantString(client.get(key2.getBytes(), null, 0, getGrpcTimeout()).get(), "nx2");
    }

    // =========================================================================
    // UNLINK / TOUCH
    // =========================================================================

    @Test
    @DisplayName("Cluster: UNLINK in Redis")
    void testUnlinkInCluster() throws Exception {
        String key = uniqueKeyWithTag("cluster_unlink");

        jedis.set(key, "to_unlink");
        assertEquals(1L, jedis.unlink(key));

        awaitReplication();
        assertFalse(client.containsKey(key.getBytes(), null, 0, getGrpcTimeout()).get());
    }

    @Test
    @DisplayName("Cluster: TOUCH in Redis")
    void testTouchInCluster() throws Exception {
        String key = uniqueKeyWithTag("cluster_touch");

        jedis.set(key, "touch_value");
        awaitReplication();
        assertEquals(1L, jedis.touch(key));
        assertEquals(0L, jedis.touch(uniqueKeyWithTag("cluster_touch_missing")));
    }

    // =========================================================================
    // TTL: PEXPIRE / PTTL / EXPIREAT / PEXPIREAT
    // =========================================================================

    @Test
    @DisplayName("Cluster: PEXPIRE + PTTL in Redis")
    void testPexpirePttlInCluster() throws Exception {
        String key = uniqueKeyWithTag("cluster_pexpire");

        jedis.set(key, "pexpire_value");
        assertEquals(1L, jedis.pexpire(key, 300000));

        awaitReplication();
        long pttl = jedis.pttl(key);
        assertTrue(pttl > 0 && pttl <= 300000);
    }

    @Test
    @DisplayName("Cluster: EXPIREAT/PEXPIREAT in Redis")
    void testExpireAtInCluster() throws Exception {
        String key = uniqueKeyWithTag("cluster_expireat");

        long futureSec = System.currentTimeMillis() / 1000 + 600;
        long futureMs = System.currentTimeMillis() + 600000;

        jedis.set(key, "expireat_value");
        assertEquals(1L, jedis.expireAt(key, futureSec));

        String key2 = uniqueKeyWithTag("cluster_pexpireat");
        jedis.set(key2, "pexpireat_value");
        assertEquals(1L, jedis.pexpireAt(key2, futureMs));

        awaitReplication();
        assertTrue(jedis.ttl(key) > 0);
        assertTrue(jedis.pttl(key2) > 0);
    }

    // =========================================================================
    // GETRANGE / SETRANGE
    // =========================================================================

    @Test
    @DisplayName("Cluster: GETRANGE/SETRANGE in Redis")
    void testGetSetRangeInCluster() throws Exception {
        String key = uniqueKeyWithTag("cluster_range");

        jedis.set(key, "hello world");
        assertEquals("hello", jedis.getrange(key, 0, 4));

        jedis.setrange(key, 6, "redis");
        awaitReplication();
        assertEquals("hello redis", jedis.get(key));
    }
}