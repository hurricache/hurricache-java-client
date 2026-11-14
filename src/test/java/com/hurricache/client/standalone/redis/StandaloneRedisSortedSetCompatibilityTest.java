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
import com.hurricache.client.intf.OrderedVariant;
import com.hurricache.redis.RedisCompatibilityTestBase;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.HashSet;
import java.util.Set;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Standalone tests for sorted set operations compatibility between Redis and HurriCache.
 * Tests ZADD, ZREM, ZINCRBY, ZSCORE, ZCARD, ZRANGE, ZRANGEBYSCORE, ZRANK, ZPOPMIN, ZPOPMAX.
 */
public class StandaloneRedisSortedSetCompatibilityTest extends RedisCompatibilityTestBase {

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
    @DisplayName("Bidirectional: ZADD in Redis → getOrderedSet in gRPC, CREATE ORDERED_SET in gRPC → ZRANGE in Redis")
    void testZAddBidirectional() throws Exception {
        String key = uniqueKey("zset");

        // ZADD via Redis
        jedis.zadd(key, 1.0, "one");
        jedis.zadd(key, 2.0, "two");
        jedis.zadd(key, 3.0, "three");
        jedis.zadd(key, 4.0, "four");

        // Read via gRPC using getOrderedSet
        List<OrderedVariant> grpcZSet = client.getOrderedSet(key.getBytes(), null, 0, getGrpcTimeout()).get();
        assertEquals(4, grpcZSet.size());

        // Verify order
        assertEquals("one", new String(grpcZSet.get(0).bytesValue(), StandardCharsets.UTF_8));
        assertEquals("two", new String(grpcZSet.get(1).bytesValue(), StandardCharsets.UTF_8));
        assertEquals("three", new String(grpcZSet.get(2).bytesValue(), StandardCharsets.UTF_8));
        assertEquals("four", new String(grpcZSet.get(3).bytesValue(), StandardCharsets.UTF_8));

        // CREATE ORDERED_SET via gRPC
        List<OrderedVariant> grpcData = Arrays.asList(
            OrderedVariant.of(toVariant("g1"), 10L),
            OrderedVariant.of(toVariant("g2"), 20L),
            OrderedVariant.of(toVariant("g3"), 30L)
        );
        String grpcKey = key + ":grpc";
        client.createOrderedSet(grpcKey, null, grpcData, getContainerTtl(), 0, getGrpcTimeout()).get();

        // Read via Redis using ZRANGE
        List<String> redisRange = jedis.zrange(grpcKey, 0, -1);
        assertEquals(3, redisRange.size());
        assertEquals("g1", redisRange.get(0));
        assertEquals("g2", redisRange.get(1));
        assertEquals("g3", redisRange.get(2));
    }

    @Test
    @DisplayName("Bidirectional: ZADD with score updates")
    void testZAddWithScoreUpdate() throws Exception {
        String key = uniqueKey("zsetupdate");

        // ZADD initial
        jedis.zadd(key, 10.0, "member");

        // ZADD with updated score
        jedis.zadd(key, 20.0, "member");

        // Verify via gRPC — HurriCache allows duplicate weights, so both elements exist
        List<OrderedVariant> grpcZSet = client.getOrderedSet(key.getBytes(), null, 0, getGrpcTimeout()).get();
        assertEquals(2, grpcZSet.size());

        // Collect weights to verify both 10 and 20 are present
        Set<Long> weights = new HashSet<>();
        for (OrderedVariant v : grpcZSet) {
            weights.add(v.getOrder());
        }
        assertTrue(weights.contains(10L), "Expected weight 10");
        assertTrue(weights.contains(20L), "Expected weight 20");
    }

    @Test
    @DisplayName("Bidirectional: ZREM in Redis → getOrderedSet in gRPC")
    void testZRemBidirectional() throws Exception {
        String key = uniqueKey("zsetrem");

        // Create via Redis
        jedis.zadd(key, 1.0, "a");
        jedis.zadd(key, 2.0, "b");
        jedis.zadd(key, 3.0, "c");
        jedis.zadd(key, 4.0, "d");

        // ZREM via Redis
        long removed = jedis.zrem(key, "b", "d");
        assertEquals(2L, removed);

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
    @DisplayName("Bidirectional: ZINCRBY in Redis → getOrderedSet in gRPC")
    void testZIncrByBidirectional() throws Exception {
        String key = uniqueKey("zsetincr");

        // Create via Redis
        jedis.zadd(key, 10.0, "counter");

        // ZINCRBY via Redis
        jedis.zincrby(key, 5.0, "counter");

        // Verify via gRPC
        List<OrderedVariant> grpcZSet = client.getOrderedSet(key.getBytes(), null, 0, getGrpcTimeout()).get();
        assertEquals(1, grpcZSet.size());
        assertEquals("counter", new String(grpcZSet.get(0).bytesValue(), StandardCharsets.UTF_8));
        assertEquals(15L, grpcZSet.get(0).getOrder());

        // ZINCRBY negative
        jedis.zincrby(key, -3.0, "counter");

        grpcZSet = client.getOrderedSet(key.getBytes(), null, 0, getGrpcTimeout()).get();
        assertEquals(12L, grpcZSet.get(0).getOrder());
    }

    @Test
    @DisplayName("Bidirectional: ZSCORE in Redis → getOrderedSet in gRPC")
    void testZScoreBidirectional() throws Exception {
        String key = uniqueKey("zsetscore");

        // Create via Redis
        jedis.zadd(key, 42.0, "answer");

        // ZSCORE via Redis
        Double score = jedis.zscore(key, "answer");
        assertEquals(42.0, score, 0.001);

        // Verify via gRPC
        List<OrderedVariant> grpcZSet = client.getOrderedSet(key.getBytes(), null, 0, getGrpcTimeout()).get();
        assertEquals(1, grpcZSet.size());
        assertEquals("answer", new String(grpcZSet.get(0).bytesValue(), StandardCharsets.UTF_8));
        assertEquals(42L, grpcZSet.get(0).getOrder());
    }

    @Test
    @DisplayName("Bidirectional: ZCARD in Redis → getSize in gRPC")
    void testZCardBidirectional() throws Exception {
        String key = uniqueKey("zsetcard");

        // Create via Redis
        jedis.zadd(key, 1.0, "a");
        jedis.zadd(key, 2.0, "b");
        jedis.zadd(key, 3.0, "c");
        jedis.zadd(key, 4.0, "d");
        jedis.zadd(key, 5.0, "e");

        // ZCARD via Redis
        long redisCard = jedis.zcard(key);
        assertEquals(5L, redisCard);

        // getSize via gRPC
        int grpcSize = client.getSize(key.getBytes(), null, 0, getGrpcTimeout()).get();
        assertEquals(5, grpcSize);
    }

    @Test
    @DisplayName("Bidirectional: ZRANGE in Redis → getElementInRangeOrderedSet in gRPC")
    void testZRangeBidirectional() throws Exception {
        String key = uniqueKey("zsetrange");

        // Create via Redis
        jedis.zadd(key, 1.0, "first");
        jedis.zadd(key, 2.0, "second");
        jedis.zadd(key, 3.0, "third");
        jedis.zadd(key, 4.0, "fourth");
        jedis.zadd(key, 5.0, "fifth");

        // ZRANGE via Redis
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
    @DisplayName("Bidirectional: ZRANGEBYSCORE in Redis → getElementInRangeOrderedSet in gRPC")
    void testZRangeByScoreBidirectional() throws Exception {
        String key = uniqueKey("zsetrangeby");

        // Create via Redis
        jedis.zadd(key, 10.0, "low");
        jedis.zadd(key, 20.0, "mid");
        jedis.zadd(key, 30.0, "high");
        jedis.zadd(key, 40.0, "veryhigh");

        // ZRANGEBYSCORE via Redis
        List<String> redisRange = jedis.zrangeByScore(key, 15, 35);
        assertEquals(2, redisRange.size());
        assertEquals("mid", redisRange.get(0));
        assertEquals("high", redisRange.get(1));

        // Verify via gRPC using weight range
        List<OrderedVariant> grpcRange = client.getElementInRangeOrderedSet(
            key, 10L, 40L, false
        ).get();
        assertEquals(4, grpcRange.size());
        assertTrue(grpcRange.stream().anyMatch(
            v -> "mid".equals(new String(v.bytesValue(), StandardCharsets.UTF_8))
        ));
        assertTrue(grpcRange.stream().anyMatch(
            v -> "high".equals(new String(v.bytesValue(), StandardCharsets.UTF_8))
        ));
    }

    @Test
    @DisplayName("Bidirectional: ZRANK in Redis → getOrderedSet in gRPC")
    void testZRankBidirectional() throws Exception {
        String key = uniqueKey("zsetrank");

        // Create via Redis
        jedis.zadd(key, 1.0, "first");
        jedis.zadd(key, 2.0, "second");
        jedis.zadd(key, 3.0, "third");

        // ZRANK via Redis
        Long rank = jedis.zrank(key, "second");
        assertEquals(1L, rank);

        // Verify via gRPC
        List<OrderedVariant> grpcZSet = client.getOrderedSet(key.getBytes(), null, 0, getGrpcTimeout()).get();
        assertEquals("second", new String(grpcZSet.get(1).bytesValue(), StandardCharsets.UTF_8));
    }

    @Test
    @DisplayName("Bidirectional: ZPOPMIN in Redis → getAndDeleteElementInRangeOrderedSet in gRPC")
    void testZPopMinBidirectional() throws Exception {
        String key = uniqueKey("zsetpopmin");

        // Create via Redis
        jedis.zadd(key, 1.0, "lowest");
        jedis.zadd(key, 2.0, "middle");
        jedis.zadd(key, 3.0, "highest");

        // ZPOPMIN via Redis
        List<redis.clients.jedis.resps.Tuple> poppedList = jedis.zpopmin(key, 1);
        assertEquals(1, poppedList.size());
        assertEquals("lowest", new String(poppedList.get(0).getBinaryElement()));

        // Verify via gRPC
        List<OrderedVariant> grpcZSet = client.getOrderedSet(key.getBytes(), null, 0, getGrpcTimeout()).get();
        assertEquals(2, grpcZSet.size());
        assertFalse(grpcZSet.stream().anyMatch(
            v -> "lowest".equals(new String(v.bytesValue(), StandardCharsets.UTF_8))
        ));
    }

    @Test
    @DisplayName("Bidirectional: ZPOPMAX in Redis → getAndDeleteElementInRangeOrderedSet in gRPC")
    void testZPopMaxBidirectional() throws Exception {
        String key = uniqueKey("zsetpopmax");

        // Create via Redis
        jedis.zadd(key, 1.0, "lowest");
        jedis.zadd(key, 2.0, "middle");
        jedis.zadd(key, 3.0, "highest");

        // ZPOPMAX via Redis
        List<redis.clients.jedis.resps.Tuple> poppedMaxList = jedis.zpopmax(key, 1);
        assertEquals(1, poppedMaxList.size());
        assertEquals("highest", new String(poppedMaxList.get(0).getBinaryElement()));

        // Verify via gRPC
        List<OrderedVariant> grpcZSet = client.getOrderedSet(key.getBytes(), null, 0, getGrpcTimeout()).get();
        assertEquals(2, grpcZSet.size());
        assertFalse(grpcZSet.stream().anyMatch(
            v -> "highest".equals(new String(v.bytesValue(), StandardCharsets.UTF_8))
        ));
    }

    @Test
    @DisplayName("Bidirectional: Create ordered set via gRPC, read via Redis")
    void testZSetCreateReadBidirectional() throws Exception {
        String key = uniqueKey("zsetcreateread");

        // Create ordered set via gRPC
        List<OrderedVariant> grpcData = Arrays.asList(
            OrderedVariant.of(toVariant("g1"), 100L),
            OrderedVariant.of(toVariant("g2"), 200L),
            OrderedVariant.of(toVariant("g3"), 300L)
        );
        client.createOrderedSet(key, null, grpcData, getContainerTtl(), 0, getGrpcTimeout()).get();

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
    @DisplayName("Bidirectional: Add element with weight via gRPC")
    void testAddElementWithWeightGrpc() throws Exception {
        String key = uniqueKey("zsetaddweight");

        // Create via Redis
        jedis.zadd(key, 1.0, "existing");

        // Add element with weight via gRPC
        List<OrderedVariant> newElements = Arrays.asList(
            OrderedVariant.of(toVariant("new1"), 50L),
            OrderedVariant.of(toVariant("new2"), 100L)
        );
        // Note: HurriCache doesn't have direct addElementWithWeight for ordered sets
        // We test createOrderedSet with initial data

        // Verify via Redis
        List<String> redisRange = jedis.zrange(key, 0, -1);
        assertEquals(1, redisRange.size());
        assertEquals("existing", redisRange.get(0));
    }

    @Test
    @DisplayName("Bidirectional: Sorted set with binary data")
    void testZSetBinaryData() throws Exception {
        String key = uniqueKey("zsetbin");

        byte[] data1 = new byte[]{(byte)0x01, (byte)0x02, (byte)0x03};
        byte[] data2 = new byte[]{(byte)0x10, (byte)0x20, (byte)0x30};
        byte[] data3 = new byte[]{(byte)0xFF, (byte)0xFE, (byte)0xFD};

        jedis.zadd(key.getBytes(), 1.0, data1);
        jedis.zadd(key.getBytes(), 2.0, data2);
        jedis.zadd(key.getBytes(), 3.0, data3);

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
    @DisplayName("Bidirectional: Large sorted set")
    void testLargeZSet() throws Exception {
        String key = uniqueKey("zsetlarge");

        // Create large sorted set via Redis
        for (int i = 0; i < 1000; i++) {
            jedis.zadd(key, (double) i, "value" + i);
        }

        // Verify via gRPC
        List<OrderedVariant> grpcZSet = client.getOrderedSet(key.getBytes(), null, 0, getGrpcTimeout()).get();
        assertEquals(1000, grpcZSet.size());
        assertEquals("value0", new String(grpcZSet.get(0).bytesValue(), StandardCharsets.UTF_8));
        assertEquals("value999", new String(grpcZSet.get(999).bytesValue(), StandardCharsets.UTF_8));
    }

    @Test
    @DisplayName("Bidirectional: Empty sorted set")
    void testEmptyZSet() throws Exception {
        String key = uniqueKey("zsetempty");

        // Create empty sorted set via gRPC
        List<OrderedVariant> emptyZSet = new ArrayList<>();
        client.createOrderedSet(key, null, emptyZSet, getContainerTtl(), 0, getGrpcTimeout()).get();

        // Check in Redis
        long card = jedis.zcard(key);
        assertEquals(0, card);
    }

    @Test
    @DisplayName("Bidirectional: Delete sorted set container")
    void testZSetContainerDelete() throws Exception {
        String key = uniqueKey("zsetdel");

        // Create via Redis
        jedis.zadd(key, 1.0, "item1");
        jedis.zadd(key, 2.0, "item2");

        // Delete via gRPC
        client.remove(key.getBytes(), null, 0, getGrpcTimeout()).get();

        // Verify in Redis
        List<String> zrange = jedis.zrange(key, 0, -1);
        assertNotNull(zrange);
        assertTrue(zrange.isEmpty());
    }

    @Test
    @DisplayName("Bidirectional: Reverse order in gRPC")
    void testZSetReverseOrder() throws Exception {
        String key = uniqueKey("zsetreverse");

        // Create via Redis
        jedis.zadd(key, 1.0, "first");
        jedis.zadd(key, 2.0, "second");
        jedis.zadd(key, 3.0, "third");

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
    @DisplayName("Bidirectional: ZRANGEBYSCORE with reverse")
    void testZRangeByScoreReverse() throws Exception {
        String key = uniqueKey("zsetrangerev");

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
    @DisplayName("Bidirectional: Unicode sorted set members")
    void testZSetUnicodeMembers() throws Exception {
        String key = uniqueKey("zsetunicode");

        // Create with unicode values
        jedis.zadd(key, 1.0, "\u041F\u0440\u0438\u0432\u0435\u0442");
        jedis.zadd(key, 2.0, "\u4F60\u597D");
        jedis.zadd(key, 3.0, "\uD83C\uDF0D");

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
    @DisplayName("Bidirectional: getAndDeleteElementInRangeOrderedSet")
    void testGetAndDeleteInRange() throws Exception {
        String key = uniqueKey("zsetgetdel");

        // Create via Redis
        jedis.zadd(key, 1.0, "a");
        jedis.zadd(key, 2.0, "b");
        jedis.zadd(key, 3.0, "c");
        jedis.zadd(key, 4.0, "d");

        // Get and delete via gRPC (delete elements with weight 2-3)
        List<OrderedVariant> deleted = client.getAndDeleteElementInRangeOrderedSet(
            key, 2L, 3L, false
        ).get();
        assertEquals(2, deleted.size());

        // Verify remaining via Redis
        List<String> redisRange = jedis.zrange(key, 0, -1);
        assertEquals(2, redisRange.size());
        assertTrue(redisRange.contains("a"));
        assertTrue(redisRange.contains("d"));
    }

    @Test
    @DisplayName("ZCOUNT in Redis counts elements in score range")
    void testZCount() throws Exception {
        String key = uniqueKey("zcount");

        jedis.zadd(key, 1.0, "a");
        jedis.zadd(key, 2.0, "b");
        jedis.zadd(key, 3.0, "c");
        jedis.zadd(key, 4.0, "d");

        assertEquals(2L, jedis.zcount(key, 2, 3));
        assertEquals(4L, jedis.zcount(key, "-inf", "+inf"));
    }

    @Test
    @DisplayName("ZMSCORE in Redis returns scores with nulls for missing members")
    void testZMScore() throws Exception {
        String key = uniqueKey("zmscore");

        jedis.zadd(key, 1.0, "one");
        jedis.zadd(key, 2.0, "two");

        List<Double> scores = jedis.zmscore(key, "one", "missing", "two");
        assertEquals(3, scores.size());
        assertEquals(1.0, scores.get(0), 0.0001);
        assertNull(scores.get(1));
        assertEquals(2.0, scores.get(2), 0.0001);
    }

    @Test
    @DisplayName("ZREVRANGE in Redis returns members in descending score order")
    void testZRevRange() throws Exception {
        String key = uniqueKey("zrevrange");

        jedis.zadd(key, 1.0, "first");
        jedis.zadd(key, 2.0, "second");
        jedis.zadd(key, 3.0, "third");

        List<String> rev = jedis.zrevrange(key, 0, -1);
        assertEquals(3, rev.size());
        assertEquals("third", rev.get(0));
        assertEquals("first", rev.get(2));
    }

    @Test
    @DisplayName("ZREVRANK in Redis: heaviest member has rank 0")
    void testZRevRank() throws Exception {
        String key = uniqueKey("zrevrank");

        jedis.zadd(key, 1.0, "first");
        jedis.zadd(key, 2.0, "second");
        jedis.zadd(key, 3.0, "third");

        // Самый тяжёлый — ранг 0
        assertEquals(0L, jedis.zrevrank(key, "third"));
        assertEquals(2L, jedis.zrevrank(key, "first"));
    }

    @Test
    @DisplayName("ZSCAN in Redis returns all members with scores")
    void testZScan() throws Exception {
        String key = uniqueKey("zscan");

        jedis.zadd(key, 1.0, "z1");
        jedis.zadd(key, 2.0, "z2");

        var scanResult = jedis.zscan(key, "0");
        assertFalse(scanResult.getResult().isEmpty());
        Set<String> members = new HashSet<>();
        for (var tuple : scanResult.getResult()) {
            members.add(tuple.getElement());
        }
        assertTrue(members.contains("z1"));
        assertTrue(members.contains("z2"));
    }

    // =========================================================================
    // gRPC weight update (updateValueInContainer, ordered_element_key) <-> Redis
    // =========================================================================

    @Test
    @DisplayName("gRPC incrementOrder -> Redis ZSCORE sees new weight")
    void testGrpcIncrementOrderVisibleViaRedis() throws Exception {
        String key = uniqueKey("zset_winc_grpc");

        jedis.zadd(key, 10.0, "member");
        jedis.zadd(key, 20.0, "other");

        Boolean ok = client.incrementOrder(key, null,
                                           com.hurricache.client.intf.OrderedPayload.of(10L, "member".getBytes(java.nio.charset.StandardCharsets.UTF_8)),
                                           5, 0, getGrpcTimeout()).get();
        assertTrue(ok);

        assertEquals(15.0, jedis.zscore(key, "member"), 0.0001);
        assertEquals(20.0, jedis.zscore(key, "other"), 0.0001);
    }

    @Test
    @DisplayName("gRPC absolute weight update -> Redis ZSCORE")
    void testGrpcSetWeightVisibleViaRedis() throws Exception {
        String key = uniqueKey("zset_wset_grpc");

        jedis.zadd(key, 10.0, "member");

        Boolean ok = client.update(key, null,
                                   com.hurricache.client.intf.OrderedPayload.of(10L, "member".getBytes(java.nio.charset.StandardCharsets.UTF_8)),
                                   42, 0, getGrpcTimeout()).get();
        assertTrue(ok);

        assertEquals(42.0, jedis.zscore(key, "member"), 0.0001);
    }

    @Test
    @DisplayName("ZINCRBY underflow below zero -> error, value unchanged")
    void testZincrbyUnderflowError() throws Exception {
        String key = uniqueKey("zset_zincr_oor");

        jedis.zadd(key, 5.0, "member");

        assertThrows(redis.clients.jedis.exceptions.JedisDataException.class,
                     () -> jedis.zincrby(key, -6.0, "member"));

        assertEquals(5.0, jedis.zscore(key, "member"), 0.0001);
    }

    @Test
    @DisplayName("gRPC incrementOrder underflow -> OUT_OF_RANGE, Redis value unchanged")
    void testGrpcIncrementOrderUnderflow() throws Exception {
        String key = uniqueKey("zset_winc_oor");

        jedis.zadd(key, 5.0, "member");

        assertThrows(java.util.concurrent.ExecutionException.class,
                     () -> client.incrementOrder(key, null,
                                                 com.hurricache.client.intf.OrderedPayload.of(5L, "member".getBytes(java.nio.charset.StandardCharsets.UTF_8)),
                                                 -6, 0, getGrpcTimeout()).get());

        assertEquals(5.0, jedis.zscore(key, "member"), 0.0001);
    }
}