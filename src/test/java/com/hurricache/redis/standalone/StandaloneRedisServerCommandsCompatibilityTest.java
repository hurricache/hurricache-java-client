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
 * Standalone tests for server commands compatibility between Redis and HurriCache.
 * Tests PING, ECHO, FLUSHDB, TYPE.
 */
public class StandaloneRedisServerCommandsCompatibilityTest extends RedisCompatibilityTestBase {

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
    @DisplayName("PING: Basic ping to Redis and HurriCache")
    void testPing() throws Exception {
        // PING via Redis
        String redisPing = jedis.ping();
        assertEquals("PONG", redisPing);

        // HurriCache should be responsive - verify by creating and reading a value
        String key = uniqueKey("ping");
        client.create(key, null, toVariant("ping_value"), getContainerTtl(), 0, getGrpcTimeout()).get();
        Variant result = client.get(key.getBytes(), null, 0, getGrpcTimeout()).get();
        assertEqualsVariantString(result, "ping_value");
    }

    @Test
    @DisplayName("PING: HurriCache client target verification")
    void testPingTarget() throws Exception {
        // Verify HurriCache client target
        String target = client.getTarget();
        assertNotNull(target);
        assertTrue(target.contains("50000"));
    }

    @Test
    @DisplayName("ECHO: Echo string in Redis")
    void testEcho() throws Exception {
        String message = "Hello HurriCache!";

        // ECHO via Redis
        String echoed = jedis.echo(message);
        assertEquals(message, echoed);

        // Verify HurriCache can store and retrieve the same message
        String key = uniqueKey("echo");
        client.create(key, null, toVariant(message), getContainerTtl(), 0, getGrpcTimeout()).get();
        Variant result = client.get(key.getBytes(), null, 0, getGrpcTimeout()).get();
        assertEqualsVariantString(result, message);
    }

    @Test
    @DisplayName("ECHO: Echo binary data")
    void testEchoBinary() throws Exception {
        byte[] binaryData = new byte[]{0x00, 0x01, 0x02, (byte)0xFF, (byte)0xFE};

        // ECHO via Redis
        byte[] echoed = jedis.echo(binaryData);
        assertArrayEquals(binaryData, echoed);

        // Verify HurriCache can store and retrieve the same binary data
        String key = uniqueKey("echobin");
        client.create(key, null, Variant.ofBytes(binaryData), getContainerTtl(), 0, getGrpcTimeout()).get();
        Variant result = client.get(key.getBytes(), null, 0, getGrpcTimeout()).get();
        assertArrayEquals(binaryData, result.bytesValue());
    }

    @Test
    @DisplayName("ECHO: Echo unicode string")
    void testEchoUnicode() throws Exception {
        String unicodeMessage = "\u041F\u0440\u0438\u0432\u0435\u0442 \u4F60\u597D \uD83C\uDF0D";

        // ECHO via Redis
        String echoed = jedis.echo(unicodeMessage);
        assertEquals(unicodeMessage, echoed);

        // Verify HurriCache can store and retrieve the same unicode message
        String key = uniqueKey("echounicode");
        client.create(key, null, toVariant(unicodeMessage), getContainerTtl(), 0, getGrpcTimeout()).get();
        Variant result = client.get(key.getBytes(), null, 0, getGrpcTimeout()).get();
        assertEqualsVariantString(result, unicodeMessage);
    }

    @Test
    @DisplayName("FLUSHDB: Flush Redis database")
    void testFlushDb() throws Exception {
        // Create some data
        String key1 = uniqueKey("flush1");
        String key2 = uniqueKey("flush2");
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
        String newKey = uniqueKey("flushnew");
        try {
            client.get(newKey.getBytes(), null, 0, getGrpcTimeout()).get();
            fail("Should throw exception for non-existent key");
        } catch (ExecutionException e) {
            assertTrue(e.getCause() instanceof StatusRuntimeException);
        }
    }

    @Test
    @DisplayName("FLUSHDB: Verify clean state after flush")
    void testFlushDbCleanState() throws Exception {
        // Create data before flush
        String key = uniqueKey("flushclean");
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
    @DisplayName("TYPE: String type detection")
    void testTypeString() throws Exception {
        String key = uniqueKey("typestr");
        jedis.set(key, "string_value");

        // TYPE via Redis
        String redisType = jedis.type(key);
        assertEquals("string", redisType);

        // Verify via HurriCache
        Variant result = client.get(key.getBytes(), null, 0, getGrpcTimeout()).get();
        assertNotNull(result);
        assertTrue(result.isBytes());
    }

    @Test
    @DisplayName("TYPE: Hash type detection")
    void testTypeHash() throws Exception {
        String key = uniqueKey("typehash");
        jedis.hset(key, "field", "value");

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
    @DisplayName("TYPE: List type detection")
    void testTypeList() throws Exception {
        String key = uniqueKey("typelist");
        jedis.rpush(key, "item1", "item2");

        // TYPE via Redis
        String redisType = jedis.type(key);
        assertEquals("list", redisType);

        // Verify via HurriCache
        List<com.hurricache.client.intf.Variant> list = 
            client.getList(key.getBytes(), null, 0, getGrpcTimeout()).get();
        assertNotNull(list);
        assertEquals(2, list.size());
    }

    @Test
    @DisplayName("TYPE: Set type detection")
    void testTypeSet() throws Exception {
        String key = uniqueKey("typeset");
        jedis.sadd(key, "member1", "member2");

        // TYPE via Redis
        String redisType = jedis.type(key);
        assertEquals("set", redisType);

        // Verify via HurriCache
        List<com.hurricache.client.intf.Variant> set = 
            client.getSet(key.getBytes(), null, 0, getGrpcTimeout()).get();
        assertNotNull(set);
        assertEquals(2, set.size());
    }

    @Test
    @DisplayName("TYPE: Sorted set type detection")
    void testTypeSortedSet() throws Exception {
        String key = uniqueKey("typesortedset");
        jedis.zadd(key, 1.0, "member1");

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
    @DisplayName("TYPE: Non-existent key returns none")
    void testTypeNonExistent() throws Exception {
        String key = uniqueKey("typenonex");

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
    @DisplayName("TYPE: Create container via gRPC, verify type in Redis")
    void testTypeCreateViaGrpc() throws Exception {
        // Create string via gRPC
        String stringKey = uniqueKey("typegrpcstr");
        client.create(stringKey, null, toVariant("grpc_string"), getContainerTtl(), 0, getGrpcTimeout()).get();

        // Verify type in Redis
        assertEquals("string", jedis.type(stringKey));

        // Create list via gRPC
        String listKey = uniqueKey("typegrpcl");
        List<com.hurricache.client.intf.Variant> listData = Arrays.asList(
            toVariant("grpc_list_item")
        );
        client.createList(listKey, null, listData, getContainerTtl(), 0, getGrpcTimeout()).get();

        // Verify type in Redis
        assertEquals("list", jedis.type(listKey));

        // Create set via gRPC
        String setKey = uniqueKey("typegrpcset");
        List<com.hurricache.client.intf.Variant> setData = Arrays.asList(
            toVariant("grpc_set_member")
        );
        client.createSet(setKey, null, setData, getContainerTtl(), 0, getGrpcTimeout()).get();

        // Verify type in Redis
        assertEquals("set", jedis.type(setKey));
    }

    @Test
    @DisplayName("Server commands: Verify client configuration")
    void testClientConfiguration() throws Exception {
        // Verify client configuration
        int clientId = client.getDefaultClientId();
        Duration timeout = client.getDefaultTimeout();
        Duration ttl = client.getDefaultTtl();

        assertNotNull(clientId);
        assertNotNull(timeout);
        assertTrue(timeout.toSeconds() > 0);
    }

    @Test
    @DisplayName("Server commands: Multiple operations after flush")
    void testMultipleOpsAfterFlush() throws Exception {
        // Flush
        jedis.flushDB();

        // Perform multiple operations
        String key1 = uniqueKey("multi1");
        String key2 = uniqueKey("multi2");
        String key3 = uniqueKey("multi3");

        jedis.set(key1, "value1");
        jedis.hset(key2, "field", "value");
        jedis.rpush(key3, "item");

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
}