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
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Cluster tests for set operations compatibility between Redis and HurriCache.
 * Tests SADD, SREM, SMEMBERS, SISMEMBER, SCARD, SPOP.
 * Uses FastCacheAsyncSmartClient for cluster mode testing.
 */
public class ClusterRedisSetCompatibilityTest extends RedisCompatibilityTestBase {

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
    @DisplayName("Cluster: SADD in Redis → getSet in gRPC with cluster key tags")
    void testSAddGetSetWithClusterTags() throws Exception {
        String key = uniqueKeyWithTag("cluster_set");

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
    }

    @Test
    @DisplayName("Cluster: CREATE SET in gRPC → SMEMBERS in Redis")
    void testCreateSetSmembersBidirectional() throws Exception {
        String key = uniqueKeyWithTag("cluster_create_set");

        // CREATE SET via gRPC
        List<Variant> grpcData = Arrays.asList(
            toVariant("grpc1"), toVariant("grpc2"), toVariant("grpc3")
        );
        client.createSet(key, null, grpcData, getContainerTtl(), 0, getGrpcTimeout()).get();

        // Read via Redis using SMEMBERS
        Set<String> redisSet = jedis.smembers(key);
        assertEquals(3, redisSet.size());
        assertTrue(redisSet.contains("grpc1"));
        assertTrue(redisSet.contains("grpc2"));
        assertTrue(redisSet.contains("grpc3"));
    }

    @Test
    @DisplayName("Cluster: SADD with duplicates → getSet in gRPC")
    void testSAddWithDuplicatesInCluster() throws Exception {
        String key = uniqueKeyWithTag("cluster_setdup");

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
    @DisplayName("Cluster: SREM in Redis → getSet in gRPC")
    void testSRemGetSetBidirectional() throws Exception {
        String key = uniqueKeyWithTag("cluster_setrem");

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
    @DisplayName("Cluster: SPOP in Redis → getSet + popAt in gRPC")
    void testSPopGetSetBidirectional() throws Exception {
        String key = uniqueKeyWithTag("cluster_setpop");

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
    @DisplayName("Cluster: SPOP count in Redis → getSet in gRPC")
    void testSPopCountGetSetBidirectional() throws Exception {
        String key = uniqueKeyWithTag("cluster_setpopcount");

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
    @DisplayName("Cluster: SMEMBERS in Redis → getSet in gRPC")
    void testSMembersGetSetBidirectional() throws Exception {
        String key = uniqueKeyWithTag("cluster_setmembers");

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
    @DisplayName("Cluster: SISMEMBER in Redis → getSet in gRPC")
    void testSIsMemberGetSetBidirectional() throws Exception {
        String key = uniqueKeyWithTag("cluster_setis");

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
    @DisplayName("Cluster: SCARD in Redis → getSize in gRPC")
    void testSCardGetSizeBidirectional() throws Exception {
        String key = uniqueKeyWithTag("cluster_setcard");

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
    @DisplayName("Cluster: SRANDMEMBER in Redis → getSet in gRPC")
    void testSRandMemberGetSetBidirectional() throws Exception {
        String key = uniqueKeyWithTag("cluster_setrand");

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
    @DisplayName("Cluster: SRANDMEMBER count in Redis → getSet in gRPC")
    void testSRandMemberCountGetSetBidirectional() throws Exception {
        String key = uniqueKeyWithTag("cluster_setrandcount");

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
    @DisplayName("Cluster: Create set via gRPC, pop via Redis")
    void testSetCreatePopBidirectional() throws Exception {
        String key = uniqueKeyWithTag("cluster_setpop");

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
    @DisplayName("Cluster: Set with binary data")
    void testSetBinaryDataInCluster() throws Exception {
        String key = uniqueKeyWithTag("cluster_setbin");

        byte[] data1 = new byte[]{(byte)0x01, (byte)0x02, (byte)0x03};
        byte[] data2 = new byte[]{(byte)0x10, (byte)0x20, (byte)0x30};
        byte[] data3 = new byte[]{(byte)0xFF, (byte)0xFE, (byte)0xFD};

        jedis.sadd(key.getBytes(), data1, data2, data3);

        // Read via gRPC
        List<Variant> grpcSet = client.getSet(key.getBytes(), null, 0, getGrpcTimeout()).get();
        assertEquals(3, grpcSet.size());

        Set<byte[]> grpcSetValues = new HashSet<>();
        for (Variant v : grpcSet) {
            grpcSetValues.add(v.bytesValue());
        }
        assertTrue(grpcSetValues.contains(data1));
        assertTrue(grpcSetValues.contains(data2));
        assertTrue(grpcSetValues.contains(data3));
    }

    @Test
    @DisplayName("Cluster: Large set")
    void testLargeSetInCluster() throws Exception {
        String key = uniqueKeyWithTag("cluster_setlarge");

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
    @DisplayName("Cluster: Empty set")
    void testEmptySetInCluster() throws Exception {
        String key = uniqueKeyWithTag("cluster_setempty");

        // Create empty set via gRPC
        List<Variant> emptySet = new ArrayList<>();
        client.createSet(key, null, emptySet, getContainerTtl(), 0, getGrpcTimeout()).get();

        // Check in Redis
        long card = jedis.scard(key);
        assertEquals(0, card);
    }

    @Test
    @DisplayName("Cluster: Delete set container")
    void testSetContainerDeleteInCluster() throws Exception {
        String key = uniqueKeyWithTag("cluster_setdel");

        // Create via Redis
        jedis.sadd(key, "item1", "item2");

        // Delete via gRPC
        client.remove(key.getBytes(), null, 0, getGrpcTimeout()).get();

        // Verify in Redis
        assertNull(jedis.smembers(key));
    }

    @Test
    @DisplayName("Cluster: SADD via gRPC addElementUnordered")
    void testSAddViaGrpcInCluster() throws Exception {
        String key = uniqueKeyWithTag("cluster_setaddgrpc");

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
    @DisplayName("Cluster: SREM via gRPC removeAt")
    void testSRemViaGrpcInCluster() throws Exception {
        String key = uniqueKeyWithTag("cluster_setremgrpc");

        // Create set via Redis
        jedis.sadd(key, "a", "b", "c", "d");

        // Get the set to find size
        List<Variant> grpcSet = client.getSet(key.getBytes(), null, 0, getGrpcTimeout()).get();
        int beforeSize = grpcSet.size();

        // Remove element at position 1 via gRPC
        client.removeAt(key, 1).get();

        int afterSize = client.getSize(key).get();
        assertEquals(beforeSize - 1, afterSize);
    }

    @Test
    @DisplayName("Cluster: Unicode set members")
    void testSetUnicodeMembersInCluster() throws Exception {
        String key = uniqueKeyWithTag("cluster_setunicode");

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

    @Test
    @DisplayName("Cluster: Multiple set operations stress")
    void testMultipleSetOpsInCluster() throws Exception {
        String baseKey = uniqueKeyWithTag("cluster_setmulti");

        // Create multiple set containers
        for (int i = 0; i < 10; i++) {
            String key = baseKey + ":" + i;
            jedis.sadd(key, "member" + i + "_1", "member" + i + "_2", "member" + i + "_3");
        }

        // Read all via gRPC
        for (int i = 0; i < 10; i++) {
            String key = baseKey + ":" + i;
            List<Variant> grpcSet = client.getSet(key.getBytes(), null, 0, getGrpcTimeout()).get();
            assertEquals(3, grpcSet.size());
        }
    }

    @Test
    @DisplayName("Cluster: Set with large values")
    void testSetLargeValuesInCluster() throws Exception {
        String key = uniqueKeyWithTag("cluster_setlargeval");

        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 500; i++) {
            sb.append("data").append(i);
        }
        String largeValue = sb.toString();

        jedis.sadd(key, "small", largeValue);

        // Verify via gRPC
        List<Variant> grpcSet = client.getSet(key.getBytes(), null, 0, getGrpcTimeout()).get();
        assertEquals(2, grpcSet.size());

        Set<String> grpcSetValues = new HashSet<>();
        for (Variant v : grpcSet) {
            grpcSetValues.add(new String(v.bytesValue(), StandardCharsets.UTF_8));
        }
        assertTrue(grpcSetValues.contains("small"));
        assertTrue(grpcSetValues.contains(largeValue));
    }
}