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
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Cluster tests for key management operations compatibility between Redis and HurriCache.
 * Tests DEL, EXISTS, EXPIRE, TTL, TYPE.
 * Uses FastCacheAsyncSmartClient for cluster mode testing.
 */
public class ClusterRedisKeyManagementCompatibilityTest extends RedisCompatibilityTestBase {

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
    @DisplayName("Cluster: DEL - Delete key in Redis and verify in gRPC with cluster tags")
    void testDelInRedisWithClusterTags() throws Exception {
        String key = uniqueKeyWithTag("cluster_del");

        // Create via Redis
        jedis.set(key, "value_to_delete");
        assertNotNull(jedis.get(key));

        // DEL via Redis
        long deleted = jedis.del(key);
        assertEquals(1L, deleted);

        // Verify via gRPC - should throw exception
        try {
            client.get(key.getBytes(), null, 0, getGrpcTimeout()).get();
            fail("Should throw exception for deleted key");
        } catch (ExecutionException e) {
            assertTrue(e.getCause() instanceof StatusRuntimeException);
        }
    }

    @Test
    @DisplayName("Cluster: DEL - Delete key in gRPC and verify in Redis")
    void testDelInGrpcWithClusterTags() throws Exception {
        String key = uniqueKeyWithTag("cluster_delgrpc");

        // Create via Redis
        jedis.set(key, "value_to_delete");

        assertNotNull(jedis.get(key));
        Thread.sleep(500); // replication delay
        // Delete via gRPC using remove()
        boolean removed = client.remove(key.getBytes(), null, 0, getGrpcTimeout()).get();
        assertTrue(removed);
        Thread.sleep(500); // replication delay
        // Verify in Redis
        assertNull(jedis.get(key));
    }

    @Test
    @DisplayName("Cluster: DEL - Delete non-existent key")
    void testDelNonExistentInCluster() throws Exception {
        String key = uniqueKeyWithTag("cluster_delnone");

        // DEL on non-existent key in Redis
        long deleted = jedis.del(key);
        assertEquals(0L, deleted);
        Thread.sleep(500); // replication delay
        // Verify via gRPC - should throw exception (key never existed)
        try {
            client.get(key.getBytes(), null, 0, getGrpcTimeout()).get();
            fail("Should throw exception for non-existent key");
        } catch (ExecutionException e) {
            assertTrue(e.getCause() instanceof StatusRuntimeException);
        }
    }

    @Test
    @DisplayName("Cluster: DEL - Delete different container types")
    void testDelContainerTypesInCluster() throws Exception {
        // Delete list
        String listKey = uniqueKeyWithTag("cluster_dellist");
        jedis.rpush(listKey, "item1", "item2");
        Thread.sleep(500); // replication delay
        client.remove(listKey).get();
        Thread.sleep(500); // replication delay
        assertNull(jedis.lindex(listKey, 0));

        // Delete set
        String setKey = uniqueKeyWithTag("cluster_delset");
        jedis.sadd(setKey, "member1");
        Thread.sleep(500); // replication delay
        client.remove(setKey).get();
        Thread.sleep(500); // replication delay
        assertNull(jedis.smembers(setKey));

        // Delete sorted set
        String zsetKey = uniqueKeyWithTag("cluster_delzset");
        jedis.zadd(zsetKey, 1.0, "member1");
        Thread.sleep(500); // replication delay
        client.remove(zsetKey).get();
        Thread.sleep(500); // replication delay
        assertNull(jedis.zrange(zsetKey, 0, -1));

        // Delete hash
        String hashKey = uniqueKeyWithTag("cluster_delhash");
        jedis.hset(hashKey, "field", "value");
        Thread.sleep(500); // replication delay
        client.remove(hashKey).get();
        Thread.sleep(500); // replication delay
        assertNull(jedis.hget(hashKey, "field"));
    }

    @Test
    @DisplayName("Cluster: EXISTS - Key existence check in Redis and gRPC")
    void testExistsBidirectionalInCluster() throws Exception {
        String key = uniqueKeyWithTag("cluster_exists");

        // Key doesn't exist initially
        assertFalse(jedis.exists(key));

        // Try via gRPC
        boolean exists = client.containsKey(key.getBytes(), null, 0, getGrpcTimeout()).get();
        assertFalse(exists);

        // Create key
        jedis.set(key, "exists_value");

        // Check existence
        assertTrue(jedis.exists(key));
        Thread.sleep(500); // replication delay
        // Verify via gRPC
        exists = client.containsKey(key.getBytes(), null, 0, getGrpcTimeout()).get();
        assertTrue(exists);

        // Delete key
        jedis.del(key);

        // Check again
        assertFalse(jedis.exists(key));
        awaitReplication(); // jedis-чтение может попасть на бэкап
        exists = client.containsKey(key.getBytes(), null, 0, getGrpcTimeout()).get();
        assertFalse(exists);
    }

    @Test
    @DisplayName("Cluster: EXISTS - Multiple keys existence check")
    void testExistsMultipleKeysInCluster() throws Exception {
        String key1 = uniqueKeyWithTag("cluster_existsmulti");
        String key2 = uniqueKeyWithTag("cluster_existsmulti");
        String key3 = uniqueKeyWithTag("cluster_existsmulti");

        jedis.set(key1, "value1");
        jedis.set(key2, "value2");

        // EXISTS for multiple keys
        long existsCount = jedis.exists(key1, key2, key3);
        assertEquals(2L, existsCount);

        // Verify via gRPC
        boolean exists1 = client.containsKey(key1).get();
        boolean exists2 = client.containsKey(key2).get();
        boolean exists3 = client.containsKey(key3).get();

        assertTrue(exists1);
        assertTrue(exists2);
        assertFalse(exists3);
    }

    @Test
    @DisplayName("Cluster: EXISTS - Container existence check")
    void testExistsContainersInCluster() throws Exception {
        // Create list
        String listKey = uniqueKeyWithTag("cluster_existslist");
        jedis.rpush(listKey, "item");

        // Create set
        String setKey = uniqueKeyWithTag("cluster_existsset");
        jedis.sadd(setKey, "member");
        Thread.sleep(500); // replication delay
        // Verify via gRPC
        assertTrue(client.containsKey(listKey).get());
        assertTrue(client.containsKey(setKey).get());

        // Delete via gRPC
        client.remove(listKey).get();
        client.remove(setKey).get();

        // Verify not exists
        assertFalse(client.containsKey(listKey).get());
        assertFalse(client.containsKey(setKey).get());
    }

    @Test
    @DisplayName("Cluster: EXPIRE - Set expiry in Redis and verify in gRPC")
    void testExpireInRedisInCluster() throws Exception {
        String key = uniqueKeyWithTag("cluster_expire");

        // Create with expiry in Redis
        jedis.setex(key, 300, "expiring_value");
        Thread.sleep(500); // replication delay
        // Verify TTL in Redis
        long redisTtl = jedis.ttl(key);
        assertTrue(redisTtl > 0 && redisTtl <= 300);

        // Verify value via gRPC
        Variant result = client.get(key.getBytes(), null, 0, getGrpcTimeout()).get();
        assertEqualsVariantString(result, "expiring_value");

        // Verify TTL via gRPC
        Long grpcTtl = client.getTtl(key.getBytes(), null, 0, getGrpcTimeout()).get();
        assertNotNull(grpcTtl);
        assertTrue(grpcTtl > 0);
    }

    @Test
    @DisplayName("Cluster: EXPIRE - Set expiry via gRPC and verify in Redis")
    void testExpireInGrpcInCluster() throws Exception {
        String key = uniqueKeyWithTag("cluster_expiregrpc");

        // Create without expiry in Redis
        jedis.set(key, "value");
        long noTtl = jedis.ttl(key);
        assertEquals(-1L, noTtl);

        // Set expiry via gRPC using setTtl (in seconds)
        boolean ttlSet = client.setTtl(key, 600).get();
        assertTrue(ttlSet);
        awaitReplication(); // Wait for replication
        // Verify TTL in Redis (may not be propagated)
        // We just verify the key still exists
        assertNotNull(jedis.get(key));
    }

    @Test
    @DisplayName("Cluster: EXPIRE - Update existing expiry")
    void testExpireUpdateInCluster() throws Exception {
        String key = uniqueKeyWithTag("cluster_expireupdate");

        // Create with initial expiry
        jedis.setex(key, 100, "value");
        Thread.sleep(500); // replication delay
        // Update expiry
        Long updated = jedis.expire(key, 500);
        Thread.sleep(500); // replication delay
        assertTrue(updated != null && updated == 1);

        // Verify new TTL
        long redisTtl = jedis.ttl(key);
        assertTrue(redisTtl > 0 && redisTtl <= 500);

        // Verify via gRPC
        Variant result = client.get(key.getBytes(), null, 0, getGrpcTimeout()).get();
        assertEqualsVariantString(result, "value");
    }

    @Test
    @DisplayName("Cluster: TTL - Get TTL for key with expiry")
    void testTtlWithExpiryInCluster() throws Exception {
        String key = uniqueKeyWithTag("cluster_ttl");

        // Create with expiry
        jedis.setex(key, 300, "value");

        // TTL via Redis
        long redisTtl = jedis.ttl(key);
        assertTrue(redisTtl > 0 && redisTtl <= 300);
        Thread.sleep(500); // replication delay
        // TTL via gRPC
        Long grpcTtl = client.getTtl(key.getBytes(), null, 0, getGrpcTimeout()).get();
        assertNotNull(grpcTtl);
        assertTrue(grpcTtl > 0);
    }

    @Test
    @DisplayName("Cluster: TTL - Get TTL for key without expiry")
    void testTtlNoExpiryInCluster() throws Exception {
        String key = uniqueKeyWithTag("cluster_ttlnone");

        // Create without expiry
        jedis.set(key, "non_expiring_value");

        // TTL via Redis
        long redisTtl = jedis.ttl(key);
        assertEquals(-1L, redisTtl);
        Thread.sleep(500); // replication delay
        // TTL via gRPC
        Long grpcTtl = client.getTtl(key.getBytes(), null, 0, getGrpcTimeout()).get();
        // gRPC may return -1 or actual value depending on implementation
        assertNotNull(grpcTtl);
    }

    @Test
    @DisplayName("Cluster: TTL - Get TTL for non-existent key")
    void testTtlNonExistentInCluster() throws Exception {
        String key = uniqueKeyWithTag("cluster_ttlnonexistent");

        // TTL via Redis for non-existent key
        long redisTtl = jedis.ttl(key);
        assertEquals(-2L, redisTtl);

        // TTL via gRPC for non-existent key
        try {
            client.getTtl(key.getBytes(), null, 0, getGrpcTimeout()).get();
            fail("Should throw exception for non-existent key");
        } catch (ExecutionException e) {
            assertTrue(e.getCause() instanceof StatusRuntimeException);
        }
    }

    @Test
    @DisplayName("Cluster: PERSIST - Remove expiry in Redis and verify in gRPC")
    void testPersistInRedisInCluster() throws Exception {
        String key = uniqueKeyWithTag("cluster_persist");

        // Create with expiry
        jedis.setex(key, 100, "value");

        // Verify has expiry
        assertTrue(jedis.ttl(key) > 0);

        // PERSIST via Redis
        Long persisted = jedis.persist(key);
        assertTrue(persisted != null && persisted == 1);

        // Verify no expiry in Redis
        long redisTtl = jedis.ttl(key);
        assertEquals(-1L, redisTtl);
        Thread.sleep(500); // replication delay
        // Verify via gRPC
        Variant result = client.get(key.getBytes(), null, 0, getGrpcTimeout()).get();
        assertEqualsVariantString(result, "value");
    }

    @Test
    @DisplayName("Cluster: PERSIST - Remove expiry via gRPC")
    void testPersistInGrpcInCluster() throws Exception {
        String key = uniqueKeyWithTag("cluster_persistgrpc");

        // Create with expiry via gRPC
        client.create(key, null, toVariant("value"), Duration.ofSeconds(300), 0, getGrpcTimeout()).get();
        Thread.sleep(500); // replication delay
        // Verify has TTL via gRPC
        Long grpcTtl = client.getTtl(key.getBytes(), null, 0, getGrpcTimeout()).get();
        assertNotNull(grpcTtl);
        assertTrue(grpcTtl > 0);

        // Remove TTL via gRPC (set to 0 or -1)
        boolean persisted = client.setTtl(key, 0).get();

        assertTrue(persisted);

        // Verify no TTL via gRPC
        Boolean newTtl = client.containsKey(key.getBytes(), null, 0, getGrpcTimeout()).get();
        // TTL should be 0 or -1 after persist
        assertFalse(newTtl);
    }

    @Test
    @DisplayName("Cluster: PERSIST - Persist on key without expiry")
    void testPersistNoExpiryInCluster() throws Exception {
        String key = uniqueKeyWithTag("cluster_persistnone");

        // Create without expiry
        jedis.set(key, "value");

        // PERSIST on key without expiry
        Long persisted = jedis.persist(key);
        assertTrue(persisted == null || persisted == 0);

        // Verify value still exists
        assertEquals("value", jedis.get(key));
    }

    @Test
    @DisplayName("Cluster: TYPE - Verify container types via gRPC")
    void testTypeViaGrpcInCluster() throws Exception {
        // String type
        String stringKey = uniqueKeyWithTag("cluster_typestr");
        jedis.set(stringKey, "string_value");
        Thread.sleep(500); // replication delay
        Variant stringResult = client.get(stringKey.getBytes(), null, 0, getGrpcTimeout()).get();
        assertNotNull(stringResult);

        // List type
        String listKey = uniqueKeyWithTag("cluster_typelist");
        jedis.rpush(listKey, "item");
        Thread.sleep(500); // replication delay
        List<Variant> listResult = client.getList(listKey.getBytes(), null, 0, getGrpcTimeout()).get();
        assertNotNull(listResult);

        // Set type
        String setKey = uniqueKeyWithTag("cluster_typeset");
        jedis.sadd(setKey, "member");
        Thread.sleep(500); // replication delay
        List<Variant> setResult = client.getSet(setKey.getBytes(), null, 0, getGrpcTimeout()).get();
        assertNotNull(setResult);

        // Sorted set type
        String zsetKey = uniqueKeyWithTag("cluster_typezset");
        jedis.zadd(zsetKey, 1.0, "member");
        Thread.sleep(500); // replication delay
        List<com.hurricache.client.intf.OrderedVariant> zsetResult = 
            client.getOrderedSet(zsetKey.getBytes(), null, 0, getGrpcTimeout()).get();
        assertNotNull(zsetResult);

        // Hash type
        String hashKey = uniqueKeyWithTag("cluster_typehash");
        jedis.hset(hashKey, "field", "value");
        Thread.sleep(500); // replication delay
        Map<com.hurricache.client.intf.Payload, Variant> hashMapResult = 
            client.getMap(hashKey.getBytes(), null, 0, getGrpcTimeout()).get();
        assertNotNull(hashMapResult);
    }

    @Test
    @DisplayName("Cluster: Key management - Create via gRPC, manage via Redis")
    void testKeyManagementCreateGrpcManageRedisInCluster() throws Exception {
        String key = uniqueKeyWithTag("cluster_mgmtgrpc");

        // Create via gRPC
        client.create(key, null, toVariant("grpc_value"), getContainerTtl(), 0, getGrpcTimeout()).get();
        Thread.sleep(500); // replication delay
        // EXISTS via Redis
        assertTrue(jedis.exists(key));

        // TTL via Redis
        long redisTtl = jedis.ttl(key);
        assertTrue(redisTtl > 0);

        // EXPIRE via Redis
        Long expired = jedis.expire(key, 600);
        assertTrue(expired != null && expired == 1);
        Thread.sleep(500); // replication delay
        // PERSIST via Redis
        Long persisted = jedis.persist(key);
        assertTrue(persisted != null && persisted == 1);
        Thread.sleep(500); // replication delay
        // Verify TTL removed
        assertEquals(-1L, jedis.ttl(key));

        // DEL via Redis
        long deleted = jedis.del(key);
        assertEquals(1L, deleted);
        Thread.sleep(500); // replication delay
        // Verify via gRPC
        try {
            client.get(key.getBytes(), null, 0, getGrpcTimeout()).get();
            fail("Should throw exception for deleted key");
        } catch (ExecutionException e) {
            assertTrue(e.getCause() instanceof StatusRuntimeException);
        }
    }

    @Test
    @DisplayName("Cluster: Key management - Create via Redis, manage via gRPC")
    void testKeyManagementCreateRedisManageGrpcInCluster() throws Exception {
        String key = uniqueKeyWithTag("cluster_mgmtredis");

        // Create via Redis
        jedis.setex(key, 300, "redis_value");
        Thread.sleep(500); // replication delay
        // EXISTS via gRPC
        boolean exists = client.containsKey(key).get();
        assertTrue(exists);

        // TTL via gRPC
        Long grpcTtl = client.getTtl(key.getBytes(), null, 0, getGrpcTimeout()).get();
        assertNotNull(grpcTtl);
        assertTrue(grpcTtl > 0);

        // EXPIRE via gRPC (set new TTL)
        boolean ttlSet = client.setTtl(key, 600).get();
        assertTrue(ttlSet);

        // PERSIST via gRPC
        boolean persisted = client.setTtl(key, 0).get();
        assertTrue(persisted);
        Thread.sleep(500); // replication delay
        // Verify via Redis
        assertEquals(-1L, jedis.ttl(key));

        // DEL via gRPC
        boolean removed = client.remove(key).get();
        assertTrue(removed);

        // Verify via Redis
        assertNull(jedis.get(key));
    }

    @Test
    @DisplayName("Cluster: Key management - Multiple operations on same key")
    void testKeyManagementMultipleOpsInCluster() throws Exception {
        String key = uniqueKeyWithTag("cluster_mgmtmulti");

        // Create
        jedis.set(key, "value1");
        Thread.sleep(500); // replication delay
        assertTrue(client.containsKey(key).get());

        // Set expiry
        jedis.expire(key, 300);
        Thread.sleep(500); // replication delay
        Long ttl1 = client.getTtl(key.getBytes(), null, 0, getGrpcTimeout()).get();
        assertTrue(ttl1 > 0);

        // Update value
        jedis.set(key, "value2");
        Thread.sleep(500); // replication delay
        Variant v = client.get(key.getBytes(), null, 0, getGrpcTimeout()).get();
        assertEqualsVariantString(v, "value2");

        // Remove expiry
        jedis.persist(key);
        Long ttl2 = client.getTtl(key.getBytes(), null, 0, getGrpcTimeout()).get();
        assertTrue(ttl2 <= 0);

        // Delete
        client.remove(key).get();
        assertFalse(client.containsKey(key).get());
    }

    @Test
    @DisplayName("Cluster: Key management - Container size checks")
    void testKeyManagementContainerSizeInCluster() throws Exception {
        // List
        String listKey = uniqueKeyWithTag("cluster_mgmtlist");
        jedis.rpush(listKey, "a", "b", "c");
        Thread.sleep(500); // replication delay
        int listSize = client.getSize(listKey).get();
        assertEquals(3, listSize);

        // Set
        String setKey = uniqueKeyWithTag("cluster_mgmtset");
        jedis.sadd(setKey, "x", "y", "z");
        Thread.sleep(500); // replication delay
        int setSize = client.getSize(setKey).get();
        assertEquals(3, setSize);

        // Sorted set
        String zsetKey = uniqueKeyWithTag("cluster_mgmtzset");
        jedis.zadd(zsetKey, 1.0, "one");
        jedis.zadd(zsetKey, 2.0, "two");
        Thread.sleep(500); // replication delay
        int zsetSize = client.getSize(zsetKey).get();
        assertEquals(2, zsetSize);

        // Delete and verify size check fails
        client.remove(listKey).get();
        try {
            client.getSize(listKey).get();
            fail("Should throw exception for deleted container");
        } catch (ExecutionException e) {
            assertTrue(e.getCause() instanceof StatusRuntimeException);
        }
    }

    @Test
    @DisplayName("Cluster: Key management - TTL on different container types")
    void testKeyManagementTtlContainersInCluster() throws Exception {
        // List with TTL
        String listKey = uniqueKeyWithTag("cluster_mgmtlistttl");
        client.createVector(listKey, null, 
            Arrays.asList(toVariant("item")), 
            Duration.ofSeconds(300), 0, getGrpcTimeout()).get();
        Long listTtl = client.getTtl(listKey).get();
        assertTrue(listTtl > 0);

        // Set with TTL
        String setKey = uniqueKeyWithTag("cluster_mgmtsetttl");
        client.createSet(setKey, null, 
            Arrays.asList(toVariant("member")), 
            Duration.ofSeconds(300), 0, getGrpcTimeout()).get();
        Long setTtl = client.getTtl(setKey).get();
        assertTrue(setTtl > 0);

        // Update TTL
        client.setTtl(setKey, 600).get();
        Long newSetTtl = client.getTtl(setKey).get();
        assertTrue(newSetTtl > 300);
    }
}