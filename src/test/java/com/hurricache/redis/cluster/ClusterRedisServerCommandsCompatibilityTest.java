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
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Cluster tests for server commands compatibility between Redis and HurriCache.
 * Tests PING, ECHO, FLUSHDB, TYPE verification.
 * Uses FastCacheAsyncSmartClient for cluster mode testing.
 */
public class ClusterRedisServerCommandsCompatibilityTest extends RedisCompatibilityTestBase {

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
    @DisplayName("Cluster: PING - Basic ping to Redis and HurriCache")
    void testPingInCluster() throws Exception {
        // PING via Redis
        String redisPing = jedis.ping();
        assertEquals("PONG", redisPing);

        // HurriCache should be responsive - verify by creating and reading a value
        String key = uniqueKeyWithTag("cluster_ping");
        client.create(key, null, toVariant("ping_value"), getContainerTtl(), 0, getGrpcTimeout()).get();
        Thread.sleep(500); // replication delay
        Variant result = client.get(key.getBytes(), null, 0, getGrpcTimeout()).get();
        assertEqualsVariantString(result, "ping_value");
    }

    @Test
    @DisplayName("Cluster: PING - HurriCache client target verification")
    void testPingTargetInCluster() throws Exception {
        // Verify HurriCache client target
        String target = client.getTarget();
        assertNotNull(target);
        assertTrue(target.contains(CLUSTER_HOST));
    }

    @Test
    @DisplayName("Cluster: ECHO - Echo string in Redis")
    void testEchoInCluster() throws Exception {
        String message = "Hello HurriCache Cluster!";

        // ECHO via Redis
        String echoed = jedis.echo(message);
        assertEquals(message, echoed);

        // Verify HurriCache can store and retrieve the same message
        String key = uniqueKeyWithTag("cluster_echo");
        client.create(key, null, toVariant(message), getContainerTtl(), 0, getGrpcTimeout()).get();
        Variant result = client.get(key.getBytes(), null, 0, getGrpcTimeout()).get();
        assertEqualsVariantString(result, message);
    }

    @Test
    @DisplayName("Cluster: ECHO - Echo binary data")
    void testEchoBinaryInCluster() throws Exception {
        byte[] binaryData = new byte[]{0x00, 0x01, 0x02, (byte)0xFF, (byte)0xFE};

        // ECHO via Redis
        byte[] echoed = jedis.echo(binaryData);
        assertArrayEquals(binaryData, echoed);

        // Verify HurriCache can store and retrieve the same binary data
        String key = uniqueKeyWithTag("cluster_echobin");
        client.create(key, null, Variant.ofBytes(binaryData), getContainerTtl(), 0, getGrpcTimeout()).get();
        awaitReplication();
        Variant result = client.get(key.getBytes(), null, 0, getGrpcTimeout()).get();
        assertArrayEquals(binaryData, result.bytesValue());
    }

    @Test
    @DisplayName("Cluster: ECHO - Echo unicode string")
    void testEchoUnicodeInCluster() throws Exception {
        String unicodeMessage = "\u041F\u0440\u0438\u0432\u0435\u0442 \u4F60\u597D \uD83C\uDF0D";

        // ECHO via Redis
        String echoed = jedis.echo(unicodeMessage);
        assertEquals(unicodeMessage, echoed);

        // Verify HurriCache can store and retrieve the same unicode message
        String key = uniqueKeyWithTag("cluster_echounicode");
        client.create(key, null, toVariant(unicodeMessage), getContainerTtl(), 0, getGrpcTimeout()).get();
        awaitReplication(); // Wait for replication
        Variant result = client.get(key.getBytes(), null, 0, getGrpcTimeout()).get();
        assertEqualsVariantString(result, unicodeMessage);
    }

    @Test
    @DisplayName("Cluster: FLUSHDB - Flush Redis database")
    @Disabled // в кластерном режиме не работает flush db надо вручную удалть на всех нодах
    void testFlushDbInCluster() throws Exception {
        // Create some data
        String key1 = uniqueKeyWithTag("cluster_flush1");
        String key2 = uniqueKeyWithTag("cluster_flush2");
        jedis.set(key1, "value1");
        jedis.set(key2, "value2");

        // Verify data exists
        assertEquals("value1", jedis.get(key1));
        assertEquals("value2", jedis.get(key2));

        // FLUSHDB via Redis
        String result = jedis.flushDB();
        assertEquals("OK", result);

        // Verify data is gone
        assertNull(jedis.get(key1));
        assertNull(jedis.get(key2));

        // HurriCache should also be clean (new keys won't find old data)
        String newKey = uniqueKeyWithTag("cluster_flushnew");
        try {
            client.get(newKey.getBytes(), null, 0, getGrpcTimeout()).get();
            fail("Should throw exception for non-existent key");
        } catch (ExecutionException e) {
            assertTrue(e.getCause() instanceof StatusRuntimeException);
        }
    }

    @Test
    @DisplayName("Cluster: FLUSHDB - Verify clean state after flush")
    void testFlushDbCleanStateInCluster() throws Exception {
        // Create data before flush
        String key = uniqueKeyWithTag("cluster_flushclean");
        jedis.set(key, "before_flush");

        // Flush
        jedis.flushDB();

        // Create new data after flush
        jedis.set(key, "after_flush");
        assertEquals("after_flush", jedis.get(key));

        // Verify HurriCache can work with new data
        Variant result = client.get(key.getBytes(), null, 0, getGrpcTimeout()).get();
        assertEqualsVariantString(result, "after_flush");
    }

    @Test
    @DisplayName("Cluster: TYPE - String type detection")
    void testTypeStringInCluster() throws Exception {
        String key = uniqueKeyWithTag("cluster_typestr");
        jedis.set(key, "string_value");

        // TYPE via Redis
        String redisType = jedis.type(key);
        assertEquals("string", redisType);
        Thread.sleep(500); // replication delay
        // Verify via HurriCache
        Variant result = client.get(key.getBytes(), null, 0, getGrpcTimeout()).get();
        assertNotNull(result);
        assertTrue(result.isBytes());
    }

    @Test
    @DisplayName("Cluster: TYPE - Hash type detection")
    void testTypeHashInCluster() throws Exception {
        String key = uniqueKeyWithTag("cluster_typehash");
        jedis.hset(key, "field", "value");
        awaitReplication(); // Wait for replication
        // TYPE via Redis
        String redisType = jedis.type(key);
        assertEquals("hash", redisType);

        // Verify via HurriCache
        Map<com.hurricache.client.intf.Payload, Variant> map = 
            client.getMap(key.getBytes(), null, 0, getGrpcTimeout()).get();
        assertNotNull(map);
        assertEquals(1, map.size());
    }

    @Test
    @DisplayName("Cluster: TYPE - List type detection")
    void testTypeListInCluster() throws Exception {
        String key = uniqueKeyWithTag("cluster_typelist");
        jedis.rpush(key, "item1", "item2");

        // TYPE via Redis
        String redisType = jedis.type(key);
        assertEquals("list", redisType);
        awaitReplication(); // Wait for replication
        // Verify via HurriCache
        List<com.hurricache.client.intf.Variant> list = 
            client.getList(key.getBytes(), null, 0, getGrpcTimeout()).get();
        assertNotNull(list);
        assertEquals(2, list.size());
    }

    @Test
    @DisplayName("Cluster: TYPE - Set type detection")
    void testTypeSetInCluster() throws Exception {
        String key = uniqueKeyWithTag("cluster_typeset");
        jedis.sadd(key, "member1", "member2");

        // TYPE via Redis
        String redisType = jedis.type(key);
        assertEquals("set", redisType);
        Thread.sleep(500); // replication delay
        // Verify via HurriCache
        List<com.hurricache.client.intf.Variant> set = 
            client.getSet(key.getBytes(), null, 0, getGrpcTimeout()).get();
        assertNotNull(set);
        assertEquals(2, set.size());
    }

    @Test
    @DisplayName("Cluster: TYPE - Sorted set type detection")
    void testTypeSortedSetInCluster() throws Exception {
        String key = uniqueKeyWithTag("cluster_typesortedset");
        jedis.zadd(key, 1.0, "member1");
        awaitReplication(); // Wait for replication
        // TYPE via Redis
        String redisType = jedis.type(key);
        assertEquals("zset", redisType);

        // Verify via HurriCache
        List<com.hurricache.client.intf.OrderedVariant> zset = 
            client.getOrderedSet(key.getBytes(), null, 0, getGrpcTimeout()).get();
        assertNotNull(zset);
        assertEquals(1, zset.size());
    }

    @Test
    @DisplayName("Cluster: TYPE - Non-existent key returns none")
    void testTypeNonExistentInCluster() throws Exception {
        String key = uniqueKeyWithTag("cluster_typenonex");

        // TYPE via Redis for non-existent key
        String redisType = jedis.type(key);
        assertEquals("none", redisType);

        // Verify via HurriCache - should throw exception
        try {
            client.get(key.getBytes(), null, 0, getGrpcTimeout()).get();
            fail("Should throw exception for non-existent key");
        } catch (ExecutionException e) {
            assertTrue(e.getCause() instanceof StatusRuntimeException);
        }
    }

    @Test
    @DisplayName("Cluster: TYPE - Create container via gRPC, verify type in Redis")
    void testTypeCreateViaGrpcInCluster() throws Exception {
        // Create string via gRPC
        String stringKey = uniqueKeyWithTag("cluster_typegrpcstr");
        client.create(stringKey, null, toVariant("grpc_string"), getContainerTtl(), 0, getGrpcTimeout()).get();
        Thread.sleep(500); // replication delay
        // Verify type in Redis
        assertEquals("string", jedis.type(stringKey));

        // Create list via gRPC
        String listKey = uniqueKeyWithTag("cluster_typegrpcl");
        List<com.hurricache.client.intf.Variant> listData = Arrays.asList(
            toVariant("grpc_list_item")
        );
        client.createVector(listKey, null, listData, getContainerTtl(), 0, getGrpcTimeout()).get();
        Thread.sleep(500); // replication delay
        // Verify type in Redis
        assertEquals("list", jedis.type(listKey));

        // Create set via gRPC
        String setKey = uniqueKeyWithTag("cluster_typegrpcset");
        List<com.hurricache.client.intf.Variant> setData = Arrays.asList(
            toVariant("grpc_set_member")
        );
        client.createSet(setKey, null, setData, getContainerTtl(), 0, getGrpcTimeout()).get();

        // Verify type in Redis
        assertEquals("set", jedis.type(setKey));
    }

    @Test
    @DisplayName("Cluster: Server commands - Verify client configuration")
    void testClientConfigurationInCluster() throws Exception {
        // Verify client configuration
        int clientId = client.getDefaultClientId();
        Duration timeout = client.getDefaultTimeout();
        Duration ttl = client.getDefaultTtl();

        assertNotNull(clientId);
        assertNotNull(timeout);
        assertTrue(timeout.toSeconds() > 0);
    }

    @Test
    @DisplayName("Cluster: Server commands - Multiple operations after flush")
    void testMultipleOpsAfterFlushInCluster() throws Exception {
        // Flush
        jedis.flushDB();

        // Perform multiple operations
        String key1 = uniqueKeyWithTag("cluster_multi1");
        String key2 = uniqueKeyWithTag("cluster_multi2");
        String key3 = uniqueKeyWithTag("cluster_multi3");

        jedis.set(key1, "value1");
        jedis.hset(key2, "field", "value");
        jedis.rpush(key3, "item");
        Thread.sleep(500); // replication delay
        // Verify all via HurriCache
        Variant v1 = client.get(key1.getBytes(), null, 0, getGrpcTimeout()).get();
        assertEqualsVariantString(v1, "value1");

        Map<com.hurricache.client.intf.Payload, Variant> map = 
            client.getMap(key2.getBytes(), null, 0, getGrpcTimeout()).get();
        assertEquals(1, map.size());

        List<com.hurricache.client.intf.Variant> list = 
            client.getList(key3.getBytes(), null, 0, getGrpcTimeout()).get();
        assertEquals(1, list.size());
    }

    @Test
    @DisplayName("Cluster: Server commands - Verify cluster connectivity")
    void testClusterConnectivityInCluster() throws Exception {
        // Test basic connectivity by creating and reading a simple value
        String key = uniqueKeyWithTag("cluster_connectivity");
        
        // Create via gRPC
        client.create(key, null, toVariant("connectivity_test"), getContainerTtl(), 0, getGrpcTimeout()).get();
        Thread.sleep(500); // replication delay
        // Read via gRPC
        Variant result = client.get(key.getBytes(), null, 0, getGrpcTimeout()).get();
        assertEqualsVariantString(result, "connectivity_test");
        // Read via Redis
        String redisValue = jedis.get(key);
        assertEquals("connectivity_test", redisValue);
    }

    @Test
    @DisplayName("Cluster: Server commands - Multiple container types after flush")
    void testMultipleContainerTypesAfterFlushInCluster() throws Exception {
        // Flush
        jedis.flushDB();

        // Create different container types
        String stringKey = uniqueKeyWithTag("cluster_strtype");
        String listKey = uniqueKeyWithTag("cluster_lsttype");
        String setKey = uniqueKeyWithTag("cluster_sttype");
        String zsetKey = uniqueKeyWithTag("cluster_zstype");
        String hashKey = uniqueKeyWithTag("cluster_hashtype");

        jedis.set(stringKey, "string_val");
        jedis.rpush(listKey, "list_item");
        jedis.sadd(setKey, "set_member");
        jedis.zadd(zsetKey, 1.0, "zset_member");
        jedis.hset(hashKey, "field", "hash_val");
        Thread.sleep(500); // replication delay
        // Verify all via HurriCache
        Variant strResult = client.get(stringKey.getBytes(), null, 0, getGrpcTimeout()).get();
        assertEqualsVariantString(strResult, "string_val");

        List<Variant> listResult = client.getList(listKey.getBytes(), null, 0, getGrpcTimeout()).get();
        assertEquals(1, listResult.size());

        List<Variant> setResult = client.getSet(setKey.getBytes(), null, 0, getGrpcTimeout()).get();
        assertEquals(1, setResult.size());

        List<com.hurricache.client.intf.OrderedVariant> zsetResult = 
            client.getOrderedSet(zsetKey.getBytes(), null, 0, getGrpcTimeout()).get();
        assertEquals(1, zsetResult.size());

        Map<com.hurricache.client.intf.Payload, Variant> hashResult = 
            client.getMap(hashKey.getBytes(), null, 0, getGrpcTimeout()).get();
        assertEquals(1, hashResult.size());
    }

    @Test
    @DisplayName("Cluster: Server commands - FLUSHDB cleanup verification")
    @Disabled // TODO: flushdb не поддерживается в кластере
    void testFlushDbCleanupVerificationInCluster() throws Exception {
        // Create multiple types of data
        String key1 = uniqueKeyWithTag("cluster_flush_verify_1");
        String key2 = uniqueKeyWithTag("cluster_flush_verify_2");
        String key3 = uniqueKeyWithTag("cluster_flush_verify_3");

        jedis.set(key1, "string");
        jedis.rpush(key2, "list");
        jedis.sadd(key3, "set");

        // Verify exists
        assertNotNull(jedis.get(key1));
        assertNotNull(jedis.lindex(key2, 0));
        assertNotNull(jedis.smembers(key3));

        // Flush
        jedis.flushDB();

        // Verify all cleaned up
        assertNull(jedis.get(key1));
        assertNull(jedis.lindex(key2, 0));
        assertNull(jedis.smembers(key3));
    }

    @Test
    @DisplayName("Cluster: Server commands - TTL persistence after operations")
    void testTtlPersistenceAfterOpsInCluster() throws Exception {
        String key = uniqueKeyWithTag("cluster_ttlpersist");

        // Create with TTL
        client.create(key, null, toVariant("value"), Duration.ofSeconds(300), 0, getGrpcTimeout()).get();
        awaitReplication();
        // Verify TTL exists
        Long ttl1 = client.getTtl(key.getBytes(), null, 0, getGrpcTimeout()).get();
        assertTrue(ttl1 > 0);

        // Read value (should not affect TTL)
        Variant result = client.get(key.getBytes(), null, 0, getGrpcTimeout()).get();
        assertEqualsVariantString(result, "value");

        // Verify TTL still exists
        Long ttl2 = client.getTtl(key.getBytes(), null, 0, getGrpcTimeout()).get();
        assertTrue(ttl2 > 0);
    }
}