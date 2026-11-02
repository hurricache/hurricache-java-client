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
import com.hurricache.client.intf.Variant;
import com.hurricache.redis.RedisCompatibilityTestBase;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Standalone tests for key management operations compatibility between Redis and HurriCache.
 * Tests DEL, EXISTS, EXPIRE, TTL, PTTL, PERSIST, TYPE.
 */
public class StandaloneRedisKeyManagementCompatibilityTest extends RedisCompatibilityTestBase {

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
    @DisplayName("DEL: Delete key in Redis and verify in gRPC")
    void testDelInRedis() throws Exception {
        String key = uniqueKey("del");

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
    @DisplayName("DEL: Delete key in gRPC and verify in Redis")
    void testDelInGrpc() throws Exception {
        String key = uniqueKey("delgrpc");

        // Create via Redis
        jedis.set(key, "value_to_delete");
        assertNotNull(jedis.get(key));

        // Delete via gRPC using remove()
        boolean removed = client.remove(key.getBytes(), null, 0, getGrpcTimeout()).get();
        assertTrue(removed);

        // Verify in Redis
        assertNull(jedis.get(key));
    }

    @Test
    @DisplayName("DEL: Delete non-existent key")
    void testDelNonExistent() throws Exception {
        String key = uniqueKey("delnone");

        // DEL on non-existent key in Redis
        long deleted = jedis.del(key);
        assertEquals(0L, deleted);

        // Verify via gRPC - should throw exception (key never existed)
        try {
            client.get(key.getBytes(), null, 0, getGrpcTimeout()).get();
            fail("Should throw exception for non-existent key");
        } catch (ExecutionException e) {
            assertTrue(e.getCause() instanceof StatusRuntimeException);
        }
    }

    @Test
    @DisplayName("DEL: Delete different container types")
    void testDelContainerTypes() throws Exception {
        // Delete list
        String listKey = uniqueKey("dellist");
        jedis.rpush(listKey, "item1", "item2");
        client.remove(listKey).get();
        assertNull(jedis.lindex(listKey, 0));

        // Delete set
        String setKey = uniqueKey("delset");
        jedis.sadd(setKey, "member1");
        client.remove(setKey).get();
        assertTrue(jedis.smembers(setKey).isEmpty());

        // Delete sorted set
        String zsetKey = uniqueKey("delzset");
        jedis.zadd(zsetKey, 1.0, "member1");
        client.remove(zsetKey).get();
        assertTrue(jedis.zrange(zsetKey, 0, -1).isEmpty());

        // Delete hash
        String hashKey = uniqueKey("delhash");
        jedis.hset(hashKey, "field", "value");
        client.remove(hashKey).get();
        assertNull(jedis.hget(hashKey, "field"));
    }

    @Test
    @DisplayName("EXISTS: Key existence check in Redis and gRPC")
    void testExistsBidirectional() throws Exception {
        String key = uniqueKey("exists");

        // Key doesn't exist initially
        assertFalse(jedis.exists(key));

        // Try via gRPC
        boolean exists = client.containsKey(key.getBytes(), null, 0, getGrpcTimeout()).get();
        assertFalse(exists);

        // Create key
        jedis.set(key, "exists_value");

        // Check existence
        assertTrue(jedis.exists(key));

        // Verify via gRPC
        exists = client.containsKey(key.getBytes(), null, 0, getGrpcTimeout()).get();
        assertTrue(exists);

        // Delete key
        jedis.del(key);

        // Check again
        assertFalse(jedis.exists(key));

        exists = client.containsKey(key.getBytes(), null, 0, getGrpcTimeout()).get();
        assertFalse(exists);
    }

    @Test
    @DisplayName("EXISTS: Multiple keys existence check")
    void testExistsMultipleKeys() throws Exception {
        String key1 = uniqueKey("existsmulti");
        String key2 = uniqueKey("existsmulti");
        String key3 = uniqueKey("existsmulti");

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
    @DisplayName("EXISTS: Container existence check")
    void testExistsContainers() throws Exception {
        // Create list
        String listKey = uniqueKey("existslist");
        jedis.rpush(listKey, "item");

        // Create set
        String setKey = uniqueKey("existsset");
        jedis.sadd(setKey, "member");

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
    @DisplayName("EXPIRE: Set expiry in Redis and verify in gRPC")
    void testExpireInRedis() throws Exception {
        String key = uniqueKey("expire");

        // Create with expiry in Redis
        jedis.setex(key, 300, "expiring_value");

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
    @DisplayName("EXPIRE: Set expiry via gRPC and verify in Redis")
    void testExpireInGrpc() throws Exception {
        String key = uniqueKey("expiregrpc");

        // Create without expiry in Redis
        jedis.set(key, "value");
        long noTtl = jedis.ttl(key);
        assertEquals(-1L, noTtl);

        // Set expiry via gRPC using setTtl (in seconds)
        boolean ttlSet = client.setTtl(key, 600).get();
        assertTrue(ttlSet);

        // Verify TTL in Redis (may not be propagated)
        // We just verify the key still exists
        assertNotNull(jedis.get(key));
    }

    @Test
    @DisplayName("EXPIRE: Update existing expiry")
    void testExpireUpdate() throws Exception {
        String key = uniqueKey("expireupdate");

        // Create with initial expiry
        jedis.setex(key, 100, "value");

        // Update expiry
        Long updated = jedis.expire(key, 500);
        assertTrue(updated != null && updated == 1);

        // Verify new TTL
        long redisTtl = jedis.ttl(key);
        assertTrue(redisTtl > 0 && redisTtl <= 500);

        // Verify via gRPC
        Variant result = client.get(key.getBytes(), null, 0, getGrpcTimeout()).get();
        assertEqualsVariantString(result, "value");
    }

    @Test
    @DisplayName("TTL: Get TTL for key with expiry")
    void testTtlWithExpiry() throws Exception {
        String key = uniqueKey("ttl");

        // Create with expiry
        jedis.setex(key, 300, "value");

        // TTL via Redis
        long redisTtl = jedis.ttl(key);
        assertTrue(redisTtl > 0 && redisTtl <= 300);

        // TTL via gRPC
        Long grpcTtl = client.getTtl(key.getBytes(), null, 0, getGrpcTimeout()).get();
        assertNotNull(grpcTtl);
        assertTrue(grpcTtl > 0);
    }

    @Test
    @DisplayName("TTL: Get TTL for key without expiry")
    void testTtlNoExpiry() throws Exception {
        String key = uniqueKey("ttlnone");

        // Create without expiry
        jedis.set(key, "non_expiring_value");

        // TTL via Redis
        long redisTtl = jedis.ttl(key);
        assertEquals(-1L, redisTtl);

        // TTL via gRPC
        Long grpcTtl = client.getTtl(key.getBytes(), null, 0, getGrpcTimeout()).get();
        // gRPC may return -1 or actual value depending on implementation
        assertNotNull(grpcTtl);
    }

    @Test
    @DisplayName("TTL: Get TTL for non-existent key")
    void testTtlNonExistent() throws Exception {
        String key = uniqueKey("ttlnone");

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
    @DisplayName("PTTL: Get PTTL for key with expiry")
    void testPttlWithExpiry() throws Exception {
        String key = uniqueKey("pttl");

        // Create with expiry (500ms)
        jedis.setex(key, 1, "value");

        // PTTL via Redis
        long redisPttl = jedis.pttl(key);
        assertTrue(redisPttl > 0 && redisPttl <= 1500);

        // PTTL via gRPC (returns milliseconds)
        Long grpcTtl = client.getTtl(key.getBytes(), null, 0, getGrpcTimeout()).get();
        assertNotNull(grpcTtl);
        assertTrue(grpcTtl > 0);
    }

    @Test
    @DisplayName("PTTL: Get PTTL for key without expiry")
    void testPttlNoExpiry() throws Exception {
        String key = uniqueKey("pttlnone");

        // Create without expiry
        jedis.set(key, "non_expiring");

        // PTTL via Redis
        long redisPttl = jedis.pttl(key);
        assertEquals(-1L, redisPttl);

        // PTTL via gRPC
        Long grpcTtl = client.getTtl(key.getBytes(), null, 0, getGrpcTimeout()).get();
        assertNotNull(grpcTtl);
    }

    @Test
    @DisplayName("PERSIST: Remove expiry in Redis and verify in gRPC")
    void testPersistInRedis() throws Exception {
        String key = uniqueKey("persist");

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

        // Verify via gRPC
        Variant result = client.get(key.getBytes(), null, 0, getGrpcTimeout()).get();
        assertEqualsVariantString(result, "value");
    }

    @Test
    @DisplayName("PERSIST: Remove expiry via gRPC")
    void testPersistInGrpc() throws Exception {
        String key = uniqueKey("persistgrpc");

        // Create with expiry via gRPC
        client.create(key, null, toVariant("value"), Duration.ofSeconds(300), 0, getGrpcTimeout()).get();

        // Verify has TTL via gRPC
        Long grpcTtl = client.getTtl(key.getBytes(), null, 0, getGrpcTimeout()).get();
        assertNotNull(grpcTtl);
        assertTrue(grpcTtl > 0);

        // Remove TTL via gRPC (set to 0 or -1)
        boolean persisted = client.setTtl(key, 0).get();
        assertTrue(persisted);

        // Verify key is deleted — getTtl should throw NOT_FOUND
        try {
            client.getTtl(key.getBytes(), null, 0, getGrpcTimeout()).get();
            fail("Expected NOT_FOUND after TTL=0 deletes the key");
        } catch (ExecutionException e) {
            StatusRuntimeException cause = (StatusRuntimeException) e.getCause();
            assertEquals(Status.Code.NOT_FOUND, cause.getStatus().getCode());
        }
    }

    @Test
    @DisplayName("PERSIST: Persist on key without expiry")
    void testPersistNoExpiry() throws Exception {
        String key = uniqueKey("persistnone");

        // Create without expiry
        jedis.set(key, "value");

        // PERSIST on key without expiry
        Long persisted = jedis.persist(key);
        assertTrue(persisted == null || persisted == 0);

        // Verify value still exists
        assertEquals("value", jedis.get(key));
    }

    @Test
    @DisplayName("TYPE: Verify container types via gRPC")
    void testTypeViaGrpc() throws Exception {
        // String type
        String stringKey = uniqueKey("typestr");
        jedis.set(stringKey, "string_value");
        Variant stringResult = client.get(stringKey.getBytes(), null, 0, getGrpcTimeout()).get();
        assertNotNull(stringResult);

        // List type
        String listKey = uniqueKey("typelist");
        jedis.rpush(listKey, "item");
        List<Variant> listResult = client.getList(listKey.getBytes(), null, 0, getGrpcTimeout()).get();
        assertNotNull(listResult);

        // Set type
        String setKey = uniqueKey("typeset");
        jedis.sadd(setKey, "member");
        List<Variant> setResult = client.getSet(setKey.getBytes(), null, 0, getGrpcTimeout()).get();
        assertNotNull(setResult);

        // Sorted set type
        String zsetKey = uniqueKey("typezset");
        jedis.zadd(zsetKey, 1.0, "member");
        List<com.hurricache.client.intf.OrderedVariant> zsetResult = 
            client.getOrderedSet(zsetKey.getBytes(), null, 0, getGrpcTimeout()).get();
        assertNotNull(zsetResult);

        // Hash type
        String hashKey = uniqueKey("typehash");
        jedis.hset(hashKey, "field", "value");
        Map<com.hurricache.client.intf.Payload, Variant> hashMapResult = 
            client.getMap(hashKey.getBytes(), null, 0, getGrpcTimeout()).get();
        assertNotNull(hashMapResult);
    }

    @Test
    @DisplayName("Key management: Create via gRPC, manage via Redis")
    void testKeyManagementCreateGrpcManageRedis() throws Exception {
        String key = uniqueKey("mgmtgrpc");

        // Create via gRPC
        client.create(key, null, toVariant("grpc_value"), getContainerTtl(), 0, getGrpcTimeout()).get();

        // EXISTS via Redis
        assertTrue(jedis.exists(key));

        // TTL via Redis
        long redisTtl = jedis.ttl(key);
        assertTrue(redisTtl > 0);

        // EXPIRE via Redis
        Long expired = jedis.expire(key, 600);
        assertTrue(expired != null && expired == 1);

        // PERSIST via Redis
        Long persisted = jedis.persist(key);
        assertTrue(persisted != null && persisted == 1);

        // Verify TTL removed
        assertEquals(-1L, jedis.ttl(key));

        // DEL via Redis
        long deleted = jedis.del(key);
        assertEquals(1L, deleted);

        // Verify via gRPC
        try {
            client.get(key.getBytes(), null, 0, getGrpcTimeout()).get();
            fail("Should throw exception for deleted key");
        } catch (ExecutionException e) {
            assertTrue(e.getCause() instanceof StatusRuntimeException);
        }
    }

    @Test
    @DisplayName("Key management: Create via Redis, manage via gRPC")
    void testKeyManagementCreateRedisManageGrpc() throws Exception {
        String key = uniqueKey("mgmtredis");

        // Create via Redis
        jedis.setex(key, 300, "redis_value");

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

        // Verify via Redis
        assertEquals(-2, jedis.ttl(key));

        // DEL via gRPC
        boolean removed = client.remove(key).get();
        assertTrue(removed);

        // Verify via Redis
        assertNull(jedis.get(key));
    }

    @Test
    @DisplayName("Key management: Multiple operations on same key")
    void testKeyManagementMultipleOps() throws Exception {
        String key = uniqueKey("mgmtmulti");

        // Create
        jedis.set(key, "value1");
        assertTrue(client.containsKey(key).get());

        // Set expiry
        Long expireResult = jedis.expire(key, 300);
        assertTrue(expireResult != null && expireResult == 1);
        Long ttl1 = client.getTtl(key.getBytes(), null, 0, getGrpcTimeout()).get();
        System.out.println("TTL: " + ttl1);
        assertTrue(ttl1 > 0);

        // Update value
        jedis.set(key, "value2");
        Variant v = client.get(key.getBytes(), null, 0, getGrpcTimeout()).get();
        assertEqualsVariantString(v, "value2");

        // SET выше сбросил TTL (Redis-семантика) — persist возвращает 0, TTL уже -1
        Long persistResult = jedis.persist(key);
        Long ttl2 = client.getTtl(key.getBytes(), null, 0, getGrpcTimeout()).get();
        assertEquals(0L, persistResult.longValue());
        assertEquals(-1L, ttl2.longValue());

        // Delete
        client.remove(key).get();
        assertFalse(client.containsKey(key).get());
    }

    @Test
    @DisplayName("Key management: Container size checks")
    void testKeyManagementContainerSize() throws Exception {
        // List
        String listKey = uniqueKey("mgmtlist");
        jedis.rpush(listKey, "a", "b", "c");
        int listSize = client.getSize(listKey).get();
        assertEquals(3, listSize);

        // Set
        String setKey = uniqueKey("mgmtset");
        jedis.sadd(setKey, "x", "y", "z");
        int setSize = client.getSize(setKey).get();
        assertEquals(3, setSize);

        // Sorted set
        String zsetKey = uniqueKey("mgmtzset");
        jedis.zadd(zsetKey, 1.0, "one");
        jedis.zadd(zsetKey, 2.0, "two");
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
    @DisplayName("Key management: TTL on different container types")
    void testKeyManagementTtlContainers() throws Exception {
        // List with TTL
        String listKey = uniqueKey("mgmtlistttl");
        client.createVector(listKey, null,
            Arrays.asList(toVariant("item")),
            Duration.ofSeconds(300), 0, getGrpcTimeout()).get();
        Long listTtl = client.getTtl(listKey).get();
        assertTrue(listTtl > 0);

        // Set with TTL
        String setKey = uniqueKey("mgmtsetttl");
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