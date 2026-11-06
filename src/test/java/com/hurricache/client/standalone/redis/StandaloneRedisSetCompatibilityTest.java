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
import com.hurricache.client.intf.Payload;
import com.hurricache.client.intf.Variant;
import com.hurricache.redis.RedisCompatibilityTestBase;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Standalone tests for set operations compatibility between Redis and HurriCache.
 * Tests SADD, SREM, SPOP, SMEMBERS, SISMEMBER, SCARD, SRANDMEMBER.
 */
public class StandaloneRedisSetCompatibilityTest extends RedisCompatibilityTestBase {

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
    @DisplayName("Bidirectional: SADD in Redis → getSet in gRPC, CREATE SET in gRPC → SMEMBERS in Redis")
    void testSAddBidirectional() throws Exception {
        String key = uniqueKey("set");

        // SADD via Redis
        long added = jedis.sadd(key, "member1", "member2", "member3", "member4");
        assertEquals(4L, added);

        // Read via gRPC using getSet
        List<Variant> grpcSet = client.getSet(key.getBytes(), null, 0, getGrpcTimeout()).get();
        assertEquals(4, grpcSet.size());

        Set<String> grpcSetValues = new HashSet<>();
        for (Variant v : grpcSet) {
            grpcSetValues.add(new String(v.bytesValue(), StandardCharsets.UTF_8));
        }
        assertTrue(grpcSetValues.contains("member1"));
        assertTrue(grpcSetValues.contains("member2"));
        assertTrue(grpcSetValues.contains("member3"));
        assertTrue(grpcSetValues.contains("member4"));

        // CREATE SET via gRPC
        List<Variant> grpcData = Arrays.asList(
            toVariant("grpc1"), toVariant("grpc2"), toVariant("grpc3")
        );
        String grpcKey = key + ":grpc";
        client.createSet(grpcKey, null, grpcData, getContainerTtl(), 0, getGrpcTimeout()).get();

        // Read via Redis using SMEMBERS
        Set<String> redisSet = jedis.smembers(grpcKey);
        assertEquals(3, redisSet.size());
        assertTrue(redisSet.contains("grpc1"));
        assertTrue(redisSet.contains("grpc2"));
        assertTrue(redisSet.contains("grpc3"));
    }

    @Test
    @DisplayName("Bidirectional: SADD with duplicates → getSet in gRPC")
    void testSAddWithDuplicates() throws Exception {
        String key = uniqueKey("setdup");

        // SADD with duplicates
        long added1 = jedis.sadd(key, "a", "b", "c");
        assertEquals(3L, added1);

        // Adding duplicates
        long added2 = jedis.sadd(key, "a", "b");
        assertEquals(0L, added2);

        // Verify via gRPC
        List<Variant> grpcSet = client.getSet(key.getBytes(), null, 0, getGrpcTimeout()).get();
        assertEquals(3, grpcSet.size());
    }

    @Test
    @DisplayName("Bidirectional: SREM in Redis → getSet in gRPC")
    void testSRemBidirectional() throws Exception {
        String key = uniqueKey("setrem");

        // Create set via Redis
        jedis.sadd(key, "a", "b", "c", "d");

        // SREM via Redis
        long removed = jedis.srem(key, "b", "d");
        assertEquals(2L, removed);

        // Verify via gRPC
        List<Variant> grpcSet = client.getSet(key.getBytes(), null, 0, getGrpcTimeout()).get();
        assertEquals(2, grpcSet.size());

        Set<String> grpcSetValues = new HashSet<>();
        for (Variant v : grpcSet) {
            grpcSetValues.add(new String(v.bytesValue(), StandardCharsets.UTF_8));
        }
        assertTrue(grpcSetValues.contains("a"));
        assertTrue(grpcSetValues.contains("c"));
        assertFalse(grpcSetValues.contains("b"));
        assertFalse(grpcSetValues.contains("d"));
    }

    @Test
    @DisplayName("Bidirectional: SPOP in Redis → getSet + popAt in gRPC")
    void testSPopBidirectional() throws Exception {
        String key = uniqueKey("setpop");

        // Create set via Redis
        jedis.sadd(key, "first", "second", "third");

        // SPOP via Redis
        String popped = jedis.spop(key);
        assertNotNull(popped);

        // Verify via gRPC
        List<Variant> grpcSet = client.getSet(key.getBytes(), null, 0, getGrpcTimeout()).get();
        assertEquals(2, grpcSet.size());

        Set<String> grpcSetValues = new HashSet<>();
        for (Variant v : grpcSet) {
            grpcSetValues.add(new String(v.bytesValue(), StandardCharsets.UTF_8));
        }
        assertFalse(grpcSetValues.contains(popped));
    }

    @Test
    @DisplayName("Bidirectional: SPOP count in Redis → getSet in gRPC")
    void testSPopCountBidirectional() throws Exception {
        String key = uniqueKey("setpopcount");

        // Create set via Redis
        jedis.sadd(key, "1", "2", "3", "4", "5");

        // SPOP with count
        Set<String> popped = jedis.spop(key, 2);
        assertEquals(2, popped.size());

        // Verify via gRPC
        List<Variant> grpcSet = client.getSet(key.getBytes(), null, 0, getGrpcTimeout()).get();
        assertEquals(3, grpcSet.size());
    }

    @Test
    @DisplayName("Bidirectional: SMEMBERS in Redis → getSet in gRPC")
    void testSMembersBidirectional() throws Exception {
        String key = uniqueKey("setmembers");

        // Create via Redis
        jedis.sadd(key, "alpha", "beta", "gamma", "delta");

        // SMEMBERS via Redis
        Set<String> redisMembers = jedis.smembers(key);
        assertEquals(4, redisMembers.size());

        // Read via gRPC and verify
        List<Variant> grpcSet = client.getSet(key.getBytes(), null, 0, getGrpcTimeout()).get();
        assertEquals(4, grpcSet.size());

        Set<String> grpcSetValues = new HashSet<>();
        for (Variant v : grpcSet) {
            grpcSetValues.add(new String(v.bytesValue(), StandardCharsets.UTF_8));
        }
        assertEquals(redisMembers, grpcSetValues);
    }

    @Test
    @DisplayName("Bidirectional: SISMEMBER in Redis → getSet in gRPC")
    void testSIsMemberBidirectional() throws Exception {
        String key = uniqueKey("setis");

        // Create via Redis
        jedis.sadd(key, "exists1", "exists2", "exists3");

        // SISMEMBER via Redis
        assertTrue(jedis.sismember(key, "exists1"));
        assertFalse(jedis.sismember(key, "nonexistent"));

        // Verify via gRPC
        List<Variant> grpcSet = client.getSet(key.getBytes(), null, 0, getGrpcTimeout()).get();
        Set<String> grpcSetValues = new HashSet<>();
        for (Variant v : grpcSet) {
            grpcSetValues.add(new String(v.bytesValue(), StandardCharsets.UTF_8));
        }
        assertTrue(grpcSetValues.contains("exists1"));
        assertFalse(grpcSetValues.contains("nonexistent"));
    }

    @Test
    @DisplayName("Bidirectional: SCARD in Redis → getSize in gRPC")
    void testSCardBidirectional() throws Exception {
        String key = uniqueKey("setcard");

        // Create via Redis
        jedis.sadd(key, "a", "b", "c", "d", "e");

        // SCARD via Redis
        long redisCard = jedis.scard(key);
        assertEquals(5L, redisCard);

        // getSize via gRPC
        int grpcSize = client.getSize(key.getBytes(), null, 0, getGrpcTimeout()).get();
        assertEquals(5, grpcSize);
    }

    @Test
    @DisplayName("Bidirectional: SRANDMEMBER in Redis → getSet in gRPC")
    void testSRandMemberBidirectional() throws Exception {
        String key = uniqueKey("setrand");

        // Create via Redis
        jedis.sadd(key, "one", "two", "three", "four", "five");

        // SRANDMEMBER via Redis
        String randomMember = jedis.srandmember(key);
        assertNotNull(randomMember);

        // Verify it's in the set via gRPC
        List<Variant> grpcSet = client.getSet(key.getBytes(), null, 0, getGrpcTimeout()).get();
        Set<String> grpcSetValues = new HashSet<>();
        for (Variant v : grpcSet) {
            grpcSetValues.add(new String(v.bytesValue(), StandardCharsets.UTF_8));
        }
        assertTrue(grpcSetValues.contains(randomMember));
    }

    @Test
    @DisplayName("Bidirectional: SRANDMEMBER count in Redis → getSet in gRPC")
    void testSRandMemberCountBidirectional() throws Exception {
        String key = uniqueKey("setrandcount");

        // Create via Redis
        jedis.sadd(key, "a", "b", "c", "d", "e");

        // SRANDMEMBER with count
        List<String> randomMembers = jedis.srandmember(key, 3);
        assertEquals(3, randomMembers.size());

        // Verify via gRPC
        List<Variant> grpcSet = client.getSet(key.getBytes(), null, 0, getGrpcTimeout()).get();
        assertEquals(5, grpcSet.size());
    }

    @Test
    @DisplayName("Bidirectional: Create set via gRPC, pop via Redis")
    void testSetCreatePopBidirectional() throws Exception {
        String key = uniqueKey("setpop");

        // Create set via gRPC
        List<Variant> grpcData = Arrays.asList(
            toVariant("x1"), toVariant("x2"), toVariant("x3")
        );
        client.createSet(key, null, grpcData, getContainerTtl(), 0, getGrpcTimeout()).get();

        // SPOP via Redis
        String redisPopped = jedis.spop(key);
        assertNotNull(redisPopped);
        assertTrue(Arrays.asList("x1", "x2", "x3").contains(redisPopped));

        // Verify via gRPC
        List<Variant> grpcSet = client.getSet(key.getBytes(), null, 0, getGrpcTimeout()).get();
        assertEquals(2, grpcSet.size());
    }

    @Test
    @DisplayName("Bidirectional: Set with binary data")
    void testSetBinaryData() throws Exception {
        String key = uniqueKey("setbin");

        byte[] data1 = new byte[]{(byte)0x01, (byte)0x02, (byte)0x03};
        byte[] data2 = new byte[]{(byte)0x10, (byte)0x20, (byte)0x30};
        byte[] data3 = new byte[]{(byte)0xFF, (byte)0xFE, (byte)0xFD};

        jedis.sadd(key.getBytes(), data1, data2, data3);

        // Read via gRPC
        List<Variant> grpcSet = client.getSet(key.getBytes(), null, 0, getGrpcTimeout()).get();
        assertEquals(3, grpcSet.size());

        // byte[] не имеет value-equality — сравниваем содержимое через обёртку
        Set<java.util.List<Byte>> grpcSetValues = new HashSet<>();
        for (Variant v : grpcSet) {
            byte[] bytes = v.bytesValue();
            java.util.List<Byte> wrapped = new java.util.ArrayList<>(bytes.length);
            for (byte b : bytes) wrapped.add(b);
            grpcSetValues.add(wrapped);
        }
        java.util.function.Function<byte[], java.util.List<Byte>> wrap = arr -> {
            java.util.List<Byte> l = new java.util.ArrayList<>(arr.length);
            for (byte b : arr) l.add(b);
            return l;
        };
        assertTrue(grpcSetValues.contains(wrap.apply(data1)));
        assertTrue(grpcSetValues.contains(wrap.apply(data2)));
        assertTrue(grpcSetValues.contains(wrap.apply(data3)));
    }

    @Test
    @DisplayName("Bidirectional: Large set")
    void testLargeSet() throws Exception {
        String key = uniqueKey("setlarge");

        // Create large set via Redis
        String[] values = new String[1000];
        for (int i = 0; i < 1000; i++) {
            values[i] = "value" + i;
        }
        jedis.sadd(key, values);

        // Verify via gRPC
        List<Variant> grpcSet = client.getSet(key.getBytes(), null, 0, getGrpcTimeout()).get();
        assertEquals(1000, grpcSet.size());
    }

    @Test
    @DisplayName("Bidirectional: Empty set")
    void testEmptySet() throws Exception {
        String key = uniqueKey("setempty");

        // Create empty set via gRPC
        List<Variant> emptySet = new ArrayList<>();
        client.createSet(key, null, emptySet, getContainerTtl(), 0, getGrpcTimeout()).get();

        // Check in Redis
        long card = jedis.scard(key);
        assertEquals(0, card);
    }

    @Test
    @DisplayName("Bidirectional: Delete set container")
    void testSetContainerDelete() throws Exception {
        String key = uniqueKey("setdel");

        // Create via Redis
        jedis.sadd(key, "item1", "item2");

        // Delete via gRPC
        client.remove(key.getBytes(), null, 0, getGrpcTimeout()).get();

        // Verify in Redis
        assertTrue(jedis.smembers(key).isEmpty());
    }

    @Test
    @DisplayName("Bidirectional: SADD via gRPC addElementUnordered")
    void testSAddViaGrpc() throws Exception {
        String key = uniqueKey("setaddgrpc");

        // Create set via Redis
        jedis.sadd(key, "original1", "original2");

        // Add more elements via gRPC addElementUnordered
        List<Variant> newElements = Arrays.asList(
            toVariant("added1"), toVariant("added2")
        );
        client.addElementUnordered(key, null, newElements, 0, getGrpcTimeout()).get();

        // Verify via Redis
        Set<String> redisSet = jedis.smembers(key);
        assertEquals(4, redisSet.size());
        assertTrue(redisSet.contains("original1"));
        assertTrue(redisSet.contains("original2"));
        assertTrue(redisSet.contains("added1"));
        assertTrue(redisSet.contains("added2"));
    }

    @Test
    @DisplayName("Bidirectional: SREM via gRPC removeAt")
    void testSRemViaGrpc() throws Exception {
        String key = uniqueKey("setremgrpc");

        // Create set via Redis
        jedis.sadd(key, "a", "b", "c", "d");

        // Get the set to find indices
        List<Variant> grpcSet = client.getSet(key.getBytes(), null, 0, getGrpcTimeout()).get();

        // Remove element at position 1 via gRPC (this removes by position, not value)
        // For value-based removal, we need to check the behavior
        // Note: HurriCache removeAt removes by position, not by value like Redis SREM
        // This test verifies the set size decreases
        int beforeSize = grpcSet.size();
        client.removeFromContainerByKey(key, null, Payload.of("a".getBytes(StandardCharsets.UTF_8)), 1).get();

        int afterSize = client.getSize(key).get();
        assertEquals(beforeSize - 1, afterSize);
    }

    @Test
    @DisplayName("Bidirectional: Unicode set members")
    void testSetUnicodeMembers() throws Exception {
        String key = uniqueKey("setunicode");

        // Create with unicode values
        jedis.sadd(key, "\u041F\u0440\u0438\u0432\u0435\u0442", "\u4F60\u597D", "\uD83C\uDF0D");

        // Verify via gRPC
        List<Variant> grpcSet = client.getSet(key.getBytes(), null, 0, getGrpcTimeout()).get();
        assertEquals(3, grpcSet.size());

        Set<String> grpcSetValues = new HashSet<>();
        for (Variant v : grpcSet) {
            grpcSetValues.add(new String(v.bytesValue(), StandardCharsets.UTF_8));
        }
        assertTrue(grpcSetValues.contains("\u041F\u0440\u0438\u0432\u0435\u0442"));
        assertTrue(grpcSetValues.contains("\u4F60\u597D"));
        assertTrue(grpcSetValues.contains("\uD83C\uDF0D"));
    }
}