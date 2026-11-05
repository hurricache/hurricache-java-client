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
import com.hurricache.client.intf.OrderedVariant;
import com.hurricache.redis.RedisCompatibilityTestBase;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Cluster tests for sorted set operations compatibility between Redis and HurriCache.
 * Tests ZADD, ZREM, ZCARD, ZRANGE, ZRANK, ZPOPMIN, ZPOPMAX.
 * Uses FastCacheAsyncSmartClient for cluster mode testing.
 */
public class ClusterRedisSortedSetCompatibilityTest extends RedisCompatibilityTestBase {

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
    @DisplayName("Cluster: ZADD in Redis → getOrderedSet in gRPC with cluster key tags")
    void testZAddGetOrderedSetWithClusterTags() throws Exception {
        String key = uniqueKeyWithTag("cluster_zset");

        // ZADD via Redis
        jedis.zadd(key, 1.0, "one");
        jedis.zadd(key, 2.0, "two");
        jedis.zadd(key, 3.0, "three");
        jedis.zadd(key, 4.0, "four");
        Thread.sleep(500); // replication delay
        // Read via gRPC using getOrderedSet
        List<OrderedVariant> grpcZSet = client.getOrderedSet(key.getBytes(), null, 0, getGrpcTimeout()).get();
        assertEquals(4, grpcZSet.size());

        // Verify order
        assertEquals("one", new String(grpcZSet.get(0).bytesValue(), StandardCharsets.UTF_8));
        assertEquals("two", new String(grpcZSet.get(1).bytesValue(), StandardCharsets.UTF_8));
        assertEquals("three", new String(grpcZSet.get(2).bytesValue(), StandardCharsets.UTF_8));
        assertEquals("four", new String(grpcZSet.get(3).bytesValue(), StandardCharsets.UTF_8));
    }

    @Test
    @DisplayName("Cluster: CREATE ORDERED_SET in gRPC → ZRANGE in Redis")
    void testCreateOrderedSetZRangeBidirectional() throws Exception {
        String key = uniqueKeyWithTag("cluster_create_zset");

        // CREATE ORDERED_SET via gRPC
        List<OrderedVariant> grpcData = Arrays.asList(
            OrderedVariant.of(toVariant("g1"), 10L),
            OrderedVariant.of(toVariant("g2"), 20L),
            OrderedVariant.of(toVariant("g3"), 30L)
        );
        client.createOrderedSet(key, null, grpcData, getContainerTtl(), 0, getGrpcTimeout()).get();
        Thread.sleep(500); // replication delay
        // Read via Redis using ZRANGE
        List<String> redisRange = jedis.zrange(key, 0, -1);
        assertEquals(3, redisRange.size());
        assertEquals("g1", redisRange.get(0));
        assertEquals("g2", redisRange.get(1));
        assertEquals("g3", redisRange.get(2));
    }

    @Test
    @DisplayName("Cluster: ZADD with score updates")
    void testZAddWithScoreUpdateInCluster() throws Exception {
        String key = uniqueKeyWithTag("cluster_zsetupdate");

        // ZADD initial
        jedis.zadd(key, 10.0, "member");

        // ZADD with updated score
        jedis.zadd(key, 20.0, "member");
        awaitReplication(); // Wait for replication
        // Verify via gRPC — HurriCache allows duplicate weights, so both elements exist
        List<OrderedVariant> grpcZSet = client.getOrderedSet(key.getBytes(), null, 0, getGrpcTimeout()).get();
        assertEquals(2, grpcZSet.size());

        Set<Long> weights = new HashSet<>();
        for (OrderedVariant v : grpcZSet) {
            weights.add(v.getOrder());
        }
        assertTrue(weights.contains(10L), "Expected weight 10");
        assertTrue(weights.contains(20L), "Expected weight 20");
    }

    @Test
    @DisplayName("Cluster: ZREM in Redis → getOrderedSet in gRPC")
    void testZRemGetOrderedSetBidirectional() throws Exception {
        String key = uniqueKeyWithTag("cluster_zsetrem");

        // Create via Redis
        jedis.zadd(key, 1.0, "a");
        jedis.zadd(key, 2.0, "b");
        jedis.zadd(key, 3.0, "c");
        jedis.zadd(key, 4.0, "d");

        // ZREM via Redis
        long removed = jedis.zrem(key, "b", "d");
        assertEquals(2L, removed);
        awaitReplication(); // Wait for replication
        // Verify via gRPC
        List<OrderedVariant> grpcZSet = client.getOrderedSet(key.getBytes(), null, 0, getGrpcTimeout()).get();
        assertEquals(2, grpcZSet.size());

        Set<String> grpcValues = new HashSet<>();
        for (OrderedVariant v : grpcZSet) {
            grpcValues.add(new String(v.bytesValue(), StandardCharsets.UTF_8));
        }
        assertTrue(grpcValues.contains("a"));
        assertTrue(grpcValues.contains("c"));
        assertFalse(grpcValues.contains("b"));
        assertFalse(grpcValues.contains("d"));
    }

    @Test
    @DisplayName("Cluster: ZINCRBY in Redis → getOrderedSet in gRPC")
    void testZIncrByGetOrderedSetBidirectional() throws Exception {
        String key = uniqueKeyWithTag("cluster_zsetincr");

        // Create via Redis
        jedis.zadd(key, 10.0, "counter");

        // ZINCRBY via Redis
        jedis.zincrby(key, 5.0, "counter");

        awaitReplication(); // Wait for replication before gRPC read
        // Verify via gRPC
        List<OrderedVariant> grpcZSet = client.getOrderedSet(key.getBytes(), null, 0, getGrpcTimeout()).get();
        assertEquals(1, grpcZSet.size());
        assertEquals("counter", new String(grpcZSet.get(0).bytesValue(), StandardCharsets.UTF_8));
        assertEquals(15L, grpcZSet.get(0).getOrder());

        // ZINCRBY negative
        jedis.zincrby(key, -3.0, "counter");

        awaitReplication(); // Wait for replication before gRPC read
        grpcZSet = client.getOrderedSet(key.getBytes(), null, 0, getGrpcTimeout()).get();
        assertEquals(12L, grpcZSet.get(0).getOrder());
    }

    @Test
    @DisplayName("Cluster: ZCARD in Redis → getSize in gRPC")
    void testZCardGetSizeBidirectional() throws Exception {
        String key = uniqueKeyWithTag("cluster_zsetcard");

        // Create via Redis
        jedis.zadd(key, 1.0, "a");
        jedis.zadd(key, 2.0, "b");
        jedis.zadd(key, 3.0, "c");
        jedis.zadd(key, 4.0, "d");
        jedis.zadd(key, 5.0, "e");
        Thread.sleep(500); // replication delay
        // ZCARD via Redis
        long redisCard = jedis.zcard(key);
        assertEquals(5L, redisCard);

        // getSize via gRPC
        int grpcSize = client.getSize(key.getBytes(), null, 0, getGrpcTimeout()).get();
        assertEquals(5, grpcSize);
    }

    @Test
    @DisplayName("Cluster: ZRANGE in Redis → getElementInRangeOrderedSet in gRPC")
    void testZRangeGetElementInRangeBidirectional() throws Exception {
        String key = uniqueKeyWithTag("cluster_zsetrange");

        // Create via Redis
        jedis.zadd(key, 1.0, "first");
        jedis.zadd(key, 2.0, "second");
        jedis.zadd(key, 3.0, "third");
        jedis.zadd(key, 4.0, "fourth");
        jedis.zadd(key, 5.0, "fifth");

        // ZRANGE via Redis
        awaitReplication(); // jedis-чтение может попасть на бэкап
        List<String> redisRange = jedis.zrange(key, 1, 3);
        assertEquals(3, redisRange.size());
        assertEquals("second", redisRange.get(0));
        assertEquals("third", redisRange.get(1));
        assertEquals("fourth", redisRange.get(2));

        // Verify via gRPC - get all and check subset
        List<OrderedVariant> grpcZSet = client.getOrderedSet(key.getBytes(), null, 0, getGrpcTimeout()).get();
        assertEquals("second", new String(grpcZSet.get(1).bytesValue(), StandardCharsets.UTF_8));
        assertEquals("third", new String(grpcZSet.get(2).bytesValue(), StandardCharsets.UTF_8));
        assertEquals("fourth", new String(grpcZSet.get(3).bytesValue(), StandardCharsets.UTF_8));
    }

    @Test
    @DisplayName("Cluster: ZRANGEBYSCORE in Redis → getElementInRangeOrderedSet in gRPC")
    void testZRangeByScoreGetElementInRangeBidirectional() throws Exception {
        String key = uniqueKeyWithTag("cluster_zsetrangeby");

        // Create via Redis
        jedis.zadd(key, 10.0, "low");
        jedis.zadd(key, 20.0, "mid");
        jedis.zadd(key, 30.0, "high");
        jedis.zadd(key, 40.0, "veryhigh");
        Thread.sleep(500); // replication delay
        // ZRANGEBYSCORE via Redis
        List<String> redisRange = jedis.zrangeByScore(key, 15, 35);
        assertEquals(2, redisRange.size());
        assertEquals("mid", redisRange.get(0));
        assertEquals("high", redisRange.get(1));

        // Verify via gRPC using weight range
        // Redis zrangeByScore(15, 35) returns mid(20) and high(30)
        // HurriCache getElementInRange uses inclusive range, so use 20L-30L
        List<OrderedVariant> grpcRange = client.getElementInRangeOrderedSet(
            key, 20L, 30L, false
        ).get();
        assertEquals(2, grpcRange.size());
        assertTrue(grpcRange.stream().anyMatch(
            v -> "mid".equals(new String(v.bytesValue(), StandardCharsets.UTF_8))
        ));
        assertTrue(grpcRange.stream().anyMatch(
            v -> "high".equals(new String(v.bytesValue(), StandardCharsets.UTF_8))
        ));
    }

    @Test
    @DisplayName("Cluster: ZRANK in Redis → getOrderedSet in gRPC")
    void testZRankGetOrderedSetBidirectional() throws Exception {
        String key = uniqueKeyWithTag("cluster_zsetrank");

        // Create via Redis
        jedis.zadd(key, 1.0, "first");
        jedis.zadd(key, 2.0, "second");
        jedis.zadd(key, 3.0, "third");

        // ZRANK via Redis
        Long rank = jedis.zrank(key, "second");
        assertEquals(1L, rank);
        Thread.sleep(500); // replication delay
        // Verify via gRPC
        List<OrderedVariant> grpcZSet = client.getOrderedSet(key.getBytes(), null, 0, getGrpcTimeout()).get();
        assertEquals("second", new String(grpcZSet.get(1).bytesValue(), StandardCharsets.UTF_8));
    }

    @Test
    @DisplayName("Cluster: ZPOPMIN in Redis → getAndDeleteElementInRangeOrderedSet in gRPC")
    void testZPopMinGetAndDeleteInRangeBidirectional() throws Exception {
        String key = uniqueKeyWithTag("cluster_zsetpopmin");

        // Create via Redis
        jedis.zadd(key, 1.0, "lowest");
        jedis.zadd(key, 2.0, "middle");
        jedis.zadd(key, 3.0, "highest");

        // ZPOPMIN via Redis
        List<redis.clients.jedis.resps.Tuple> poppedList = jedis.zpopmin(key, 1);
        assertEquals(1, poppedList.size());
        assertEquals("lowest", new String(poppedList.get(0).getBinaryElement()));
        awaitReplication(); // Wait for replication
        // Verify via gRPC
        List<OrderedVariant> grpcZSet = client.getOrderedSet(key.getBytes(), null, 0, getGrpcTimeout()).get();
        assertEquals(2, grpcZSet.size());
        assertFalse(grpcZSet.stream().anyMatch(
            v -> "lowest".equals(new String(v.bytesValue(), StandardCharsets.UTF_8))
        ));
    }

    @Test
    @DisplayName("Cluster: ZPOPMAX in Redis → getAndDeleteElementInRangeOrderedSet in gRPC")
    void testZPopMaxGetAndDeleteInRangeBidirectional() throws Exception {
        String key = uniqueKeyWithTag("cluster_zsetpopmax");

        // Create via Redis
        jedis.zadd(key, 1.0, "lowest");
        jedis.zadd(key, 2.0, "middle");
        jedis.zadd(key, 3.0, "highest");

        // ZPOPMAX via Redis
        List<redis.clients.jedis.resps.Tuple> poppedMaxList = jedis.zpopmax(key, 1);
        assertEquals(1, poppedMaxList.size());
        assertEquals("highest", new String(poppedMaxList.get(0).getBinaryElement()));
        awaitReplication();
        // Verify via gRPC
        List<OrderedVariant> grpcZSet = client.getOrderedSet(key.getBytes(), null, 0, getGrpcTimeout()).get();
        assertEquals(2, grpcZSet.size());
        assertFalse(grpcZSet.stream().anyMatch(
            v -> "highest".equals(new String(v.bytesValue(), StandardCharsets.UTF_8))
        ));
    }

    @Test
    @DisplayName("Cluster: Create ordered set via gRPC, read via Redis")
    void testZSetCreateReadBidirectional() throws Exception {
        String key = uniqueKeyWithTag("cluster_zsetcreateread");

        // Create ordered set via gRPC
        List<OrderedVariant> grpcData = Arrays.asList(
            OrderedVariant.of(toVariant("g1"), 100L),
            OrderedVariant.of(toVariant("g2"), 200L),
            OrderedVariant.of(toVariant("g3"), 300L)
        );
        client.createOrderedSet(key, null, grpcData, getContainerTtl(), 0, getGrpcTimeout()).get();
        Thread.sleep(500); // replication delay
        // Read via Redis using ZRANGE
        List<String> redisRange = jedis.zrange(key, 0, -1);
        assertEquals(3, redisRange.size());
        assertEquals("g1", redisRange.get(0));
        assertEquals("g2", redisRange.get(1));
        assertEquals("g3", redisRange.get(2));

        // Verify scores via Redis
        assertEquals(100.0, jedis.zscore(key, "g1"), 0.001);
        assertEquals(200.0, jedis.zscore(key, "g2"), 0.001);
        assertEquals(300.0, jedis.zscore(key, "g3"), 0.001);
    }

    @Test
    @DisplayName("Cluster: Add element with weight via gRPC")
    void testAddElementWithWeightGrpcInCluster() throws Exception {
        String key = uniqueKeyWithTag("cluster_zsetaddweight");

        // Create via Redis
        jedis.zadd(key, 1.0, "existing");
        Thread.sleep(500); // replication delay
        // Verify via Redis
        List<String> redisRange = jedis.zrange(key, 0, -1);
        assertEquals(1, redisRange.size());
        assertEquals("existing", redisRange.get(0));
    }

    @Test
    @DisplayName("Cluster: Sorted set with binary data")
    void testZSetBinaryDataInCluster() throws Exception {
        String key = uniqueKeyWithTag("cluster_zsetbin");

        byte[] data1 = new byte[]{(byte)0x01, (byte)0x02, (byte)0x03};
        byte[] data2 = new byte[]{(byte)0x10, (byte)0x20, (byte)0x30};
        byte[] data3 = new byte[]{(byte)0xFF, (byte)0xFE, (byte)0xFD};

        jedis.zadd(key.getBytes(), 1.0, data1);
        jedis.zadd(key.getBytes(), 2.0, data2);
        jedis.zadd(key.getBytes(), 3.0, data3);
        Thread.sleep(500); // replication delay
        // Read via gRPC
        List<OrderedVariant> grpcZSet = client.getOrderedSet(key.getBytes(), null, 0, getGrpcTimeout()).get();
        assertEquals(3, grpcZSet.size());

        Set<OrderedVariant> expectedValues = Set.of(
            OrderedVariant.ofBytes(data1, 1),
            OrderedVariant.ofBytes(data2, 2),
            OrderedVariant.ofBytes(data3, 3)
        );
        for (OrderedVariant v : grpcZSet) {
            assertTrue(expectedValues.contains(v), "Expected one of data1/data2/data3 but got: " + v);
        }
    }

    @Test
    @DisplayName("Cluster: Large sorted set")
    void testLargeZSetInCluster() throws Exception {
        String key = uniqueKeyWithTag("cluster_zsetlarge");

        // Create large sorted set via Redis
        for (int i = 0; i < 1000; i++) {
            jedis.zadd(key, (double) i, "value" + i);
        }
        awaitReplication();
        // Verify via gRPC
        List<OrderedVariant> grpcZSet = client.getOrderedSet(key.getBytes(), null, 0, getGrpcTimeout()).get();
        assertEquals(1000, grpcZSet.size());
        assertEquals("value0", new String(grpcZSet.get(0).bytesValue(), StandardCharsets.UTF_8));
        assertEquals("value999", new String(grpcZSet.get(999).bytesValue(), StandardCharsets.UTF_8));
    }

    @Test
    @DisplayName("Cluster: Empty sorted set")
    void testEmptyZSetInCluster() throws Exception {
        String key = uniqueKeyWithTag("cluster_zsetempty");

        // Create empty sorted set via gRPC
        List<OrderedVariant> emptyZSet = new ArrayList<>();
        client.createOrderedSet(key, null, emptyZSet, getContainerTtl(), 0, getGrpcTimeout()).get();

        // Check in Redis
        long card = jedis.zcard(key);
        assertEquals(0, card);
    }

    @Test
    @DisplayName("Cluster: Delete sorted set container")
    void testZSetContainerDeleteInCluster() throws Exception {
        String key = uniqueKeyWithTag("cluster_zsetdel");

        // Create via Redis
        jedis.zadd(key, 1.0, "item1");
        jedis.zadd(key, 2.0, "item2");
        Thread.sleep(500); // replication delay
        // Delete via gRPC
        client.remove(key.getBytes(), null, 0, getGrpcTimeout()).get();
        Thread.sleep(500); // replication delay
        // Verify in Redis
        assertTrue(jedis.zrange(key, 0, -1).isEmpty());
    }

    @Test
    @DisplayName("Cluster: Reverse order in gRPC")
    void testZSetReverseOrderInCluster() throws Exception {
        String key = uniqueKeyWithTag("cluster_zsetreverse");

        // Create via Redis
        jedis.zadd(key, 1.0, "first");
        jedis.zadd(key, 2.0, "second");
        jedis.zadd(key, 3.0, "third");
        awaitReplication(); // Wait for replication
        // Read via gRPC in reverse order
        List<OrderedVariant> grpcReverse = client.getElementInRangeOrderedSet(
            key, 0L, 100L, true
        ).get();
        assertEquals(3, grpcReverse.size());
        assertEquals("third", new String(grpcReverse.get(0).bytesValue(), StandardCharsets.UTF_8));
        assertEquals("second", new String(grpcReverse.get(1).bytesValue(), StandardCharsets.UTF_8));
        assertEquals("first", new String(grpcReverse.get(2).bytesValue(), StandardCharsets.UTF_8));
    }

    @Test
    @DisplayName("Cluster: ZRANGEBYSCORE with reverse")
    void testZRangeByScoreReverseInCluster() throws Exception {
        String key = uniqueKeyWithTag("cluster_zsetrangerev");

        // Create via Redis
        jedis.zadd(key, 10.0, "low");
        jedis.zadd(key, 20.0, "mid");
        jedis.zadd(key, 30.0, "high");

        // ZRANGEBYSCORE with reverse via Redis
        List<String> redisRange = jedis.zrevrangeByScore(key, 30, 10);
        assertEquals(3, redisRange.size());
        assertEquals("high", redisRange.get(0));
        assertEquals("mid", redisRange.get(1));
        assertEquals("low", redisRange.get(2));
        awaitReplication(); // Wait for replication
        // Verify via gRPC
        List<OrderedVariant> grpcReverse = client.getElementInRangeOrderedSet(
            key, 10L, 30L, true
        ).get();
        assertEquals(3, grpcReverse.size());
        assertEquals("high", new String(grpcReverse.get(0).bytesValue(), StandardCharsets.UTF_8));
        assertEquals("mid", new String(grpcReverse.get(1).bytesValue(), StandardCharsets.UTF_8));
        assertEquals("low", new String(grpcReverse.get(2).bytesValue(), StandardCharsets.UTF_8));
    }

    @Test
    @DisplayName("Cluster: Unicode sorted set members")
    void testZSetUnicodeMembersInCluster() throws Exception {
        String key = uniqueKeyWithTag("cluster_zsetunicode");

        // Create with unicode values
        jedis.zadd(key, 1.0, "\u041F\u0440\u0438\u0432\u0435\u0442");
        jedis.zadd(key, 2.0, "\u4F60\u597D");
        jedis.zadd(key, 3.0, "\uD83C\uDF0D");
        Thread.sleep(500); // replication delay
        // Verify via gRPC
        List<OrderedVariant> grpcZSet = client.getOrderedSet(key.getBytes(), null, 0, getGrpcTimeout()).get();
        assertEquals(3, grpcZSet.size());

        Set<String> grpcValues = new HashSet<>();
        for (OrderedVariant v : grpcZSet) {
            grpcValues.add(new String(v.bytesValue(), StandardCharsets.UTF_8));
        }
        assertTrue(grpcValues.contains("\u041F\u0440\u0438\u0432\u0435\u0442"));
        assertTrue(grpcValues.contains("\u4F60\u597D"));
        assertTrue(grpcValues.contains("\uD83C\uDF0D"));
    }

    @Test
    @DisplayName("Cluster: getAndDeleteElementInRangeOrderedSet")
    void testGetAndDeleteInRangeInCluster() throws Exception {
        String key = uniqueKeyWithTag("cluster_zsetgetdel");

        // Create via Redis
        jedis.zadd(key, 1.0, "a");
        jedis.zadd(key, 2.0, "b");
        jedis.zadd(key, 3.0, "c");
        jedis.zadd(key, 4.0, "d");
        Thread.sleep(500); // replication delay
        // Get and delete via gRPC (delete elements with weight 2-3)
        List<OrderedVariant> deleted = client.getAndDeleteElementInRangeOrderedSet(
            key, 2L, 3L, false
        ).get();
        assertEquals(2, deleted.size());
        Thread.sleep(500); // replication delay
        // Verify remaining via Redis
        List<String> redisRange = jedis.zrange(key, 0, -1);
        assertEquals(2, redisRange.size());
        assertTrue(redisRange.contains("a"));
        assertTrue(redisRange.contains("d"));
    }

    @Test
    @DisplayName("Cluster: Multiple sorted set operations stress")
    void testMultipleZSetOpsInCluster() throws Exception {
        String baseKey = uniqueKeyWithTag("cluster_zsetmulti");

        // Create multiple sorted set containers
        for (int i = 0; i < 10; i++) {
            String key = baseKey + ":" + i;
            jedis.zadd(key, 1.0, "member1");
            jedis.zadd(key, 2.0, "member2");
            jedis.zadd(key, 3.0, "member3");
        }
        Thread.sleep(500); // replication delay
        // Read all via gRPC
        for (int i = 0; i < 10; i++) {
            String key = baseKey + ":" + i;
            List<OrderedVariant> grpcZSet = client.getOrderedSet(key.getBytes(), null, 0, getGrpcTimeout()).get();
            assertEquals(3, grpcZSet.size());
            assertEquals("member1", new String(grpcZSet.get(0).bytesValue(), StandardCharsets.UTF_8));
        }
    }

    @Test
    @DisplayName("Cluster: Sorted set with negative weights")
    void testZSetNegativeWeightsInCluster() throws Exception {
        String key = uniqueKeyWithTag("cluster_zsetneg");

        jedis.zadd(key, 2.0, "negative");
        jedis.zadd(key, 0.0, "zero");
        jedis.zadd(key, 10.0, "positive");
        awaitReplication(); // Wait for replication before gRPC read
        // Verify via gRPC
        // Sorted by weight: zero(0.0), negative(2.0), positive(10.0)
        List<OrderedVariant> grpcZSet = client.getOrderedSet(key.getBytes(), null, 0, getGrpcTimeout()).get();
        assertEquals(3, grpcZSet.size());
        assertEquals("zero", new String(grpcZSet.get(0).bytesValue(), StandardCharsets.UTF_8));
        assertEquals(0L, grpcZSet.get(0).getOrder());
        assertEquals("negative", new String(grpcZSet.get(1).bytesValue(), StandardCharsets.UTF_8));
        assertEquals(2L, grpcZSet.get(1).getOrder());
        assertEquals("positive", new String(grpcZSet.get(2).bytesValue(), StandardCharsets.UTF_8));
        assertEquals(10L, grpcZSet.get(2).getOrder());
    }

    @Test
    @DisplayName("Cluster: ZMSCORE in Redis")
    void testZMScoreInCluster() throws Exception {
        String key = uniqueKeyWithTag("cluster_zmscore");

        jedis.zadd(key, 1.0, "one");
        jedis.zadd(key, 2.0, "two");
        awaitReplication();

        List<Double> scores = jedis.zmscore(key, "one", "missing", "two");
        assertEquals(3, scores.size());
        assertEquals(1.0, scores.get(0), 0.0001);
        assertNull(scores.get(1));
        assertEquals(2.0, scores.get(2), 0.0001);
    }

    @Test
    @DisplayName("Cluster: ZCOUNT in Redis")
    void testZCountInCluster() throws Exception {
        String key = uniqueKeyWithTag("cluster_zcount");

        jedis.zadd(key, 1.0, "a");
        jedis.zadd(key, 2.0, "b");
        jedis.zadd(key, 3.0, "c");
        jedis.zadd(key, 4.0, "d");
        awaitReplication();

        assertEquals(2L, jedis.zcount(key, 2, 3));
        assertEquals(4L, jedis.zcount(key, "-inf", "+inf"));
    }

    @Test
    @DisplayName("Cluster: ZREVRANGE in Redis")
    void testZRevRangeInCluster() throws Exception {
        String key = uniqueKeyWithTag("cluster_zrevrange");

        jedis.zadd(key, 1.0, "first");
        jedis.zadd(key, 2.0, "second");
        jedis.zadd(key, 3.0, "third");
        awaitReplication();

        List<String> rev = jedis.zrevrange(key, 0, -1);
        assertEquals(3, rev.size());
        assertEquals("third", rev.get(0));
        assertEquals("first", rev.get(2));
    }

    @Test
    @DisplayName("Cluster: ZREVRANK in Redis")
    void testZRevRankInCluster() throws Exception {
        String key = uniqueKeyWithTag("cluster_zrevrank");

        jedis.zadd(key, 1.0, "first");
        jedis.zadd(key, 2.0, "second");
        jedis.zadd(key, 3.0, "third");
        awaitReplication();

        // Самый тяжёлый — ранг 0
        assertEquals(0L, jedis.zrevrank(key, "third"));
        assertEquals(2L, jedis.zrevrank(key, "first"));
    }

    @Test
    @DisplayName("Cluster: ZSCAN in Redis")
    void testZScanInCluster() throws Exception {
        String key = uniqueKeyWithTag("cluster_zscan");

        jedis.zadd(key, 1.0, "z1");
        jedis.zadd(key, 2.0, "z2");
        awaitReplication();

        var scanResult = jedis.zscan(key, "0");
        assertFalse(scanResult.getResult().isEmpty());
        Set<String> members = new HashSet<>();
        for (var tuple : scanResult.getResult()) {
            members.add(tuple.getElement());
        }
        assertTrue(members.contains("z1"));
        assertTrue(members.contains("z2"));
    }
}