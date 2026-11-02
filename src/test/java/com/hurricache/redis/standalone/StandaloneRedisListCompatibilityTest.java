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
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Standalone tests for list operations compatibility between Redis and HurriCache.
 * Tests LPUSH, RPUSH, LPOP, RPOP, LLEN, LRANGE, LINDEX, LSET, LREM, LINSERT, LTRIM.
 */
public class StandaloneRedisListCompatibilityTest extends RedisCompatibilityTestBase {

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
    @DisplayName("Bidirectional: LPUSH/RPUSH in Redis → getList in gRPC, CREATE LIST in gRPC → LRANGE in Redis")
    void testLPushRPushBidirectional() throws Exception {
        String key = uniqueKey("list");

        // LPUSH/RPUSH via Redis
        jedis.lpush(key, "head3", "head2", "head1");
        jedis.rpush(key, "tail1", "tail2");

        // Current list order: head1, head2, head3, tail1, tail2
        List<String> redisList = jedis.lrange(key, 0, -1);
        assertEquals(5, redisList.size());
        assertEquals("head1", redisList.get(0));

        // Read via gRPC
        List<Variant> grpcList = client.getList(key.getBytes(), null, 0, getGrpcTimeout()).get();
        assertEquals(5, grpcList.size());
        assertEquals("head1", new String(grpcList.get(0).bytesValue()));
        assertEquals("tail2", new String(grpcList.get(4).bytesValue()));

        // CREATE LIST via gRPC
        List<Variant> grpcData = Arrays.asList(
            toVariant("g1"), toVariant("g2"), toVariant("g3")
        );
        String grpcKey = key + ":grpc";
        client.createVector(grpcKey.getBytes(), null, grpcData, getContainerTtl(), 0, getGrpcTimeout()).get();

        // Read via Redis
        List<String> redisFromGrpc = jedis.lrange(grpcKey, 0, -1);
        assertEquals(3, redisFromGrpc.size());
        assertEquals("g1", redisFromGrpc.get(0));
        assertEquals("g3", redisFromGrpc.get(2));
    }

    @Test
    @DisplayName("Bidirectional: LPUSH + LPOP in Redis → getList + popHead in gRPC")
    void testLPopRPopBidirectional() throws Exception {
        String key = uniqueKey("lpop");

        // Create list via Redis
        jedis.rpush(key, "item1", "item2", "item3");

        // LPOP via Redis
        String popped = jedis.lpop(key);
        assertEquals("item1", popped);

        // Verify via gRPC
        List<Variant> grpcList = client.getList(key.getBytes(), null, 0, getGrpcTimeout()).get();
        assertEquals(2, grpcList.size());
        assertEquals("item2", new String(grpcList.get(0).bytesValue()));

        // RPOP via Redis
        String rPopped = jedis.rpop(key);
        assertEquals("item3", rPopped);

        grpcList = client.getList(key.getBytes(), null, 0, getGrpcTimeout()).get();
        assertEquals(1, grpcList.size());
        assertEquals("item2", new String(grpcList.get(0).bytesValue()));
    }

    @Test
    @DisplayName("Bidirectional: LLEN in Redis → getSize in gRPC")
    void testLLenBidirectional() throws Exception {
        String key = uniqueKey("llen");

        jedis.rpush(key, "a", "b", "c", "d");

        long redisLen = jedis.llen(key);
        assertEquals(4L, redisLen);

        int grpcLen = client.getSize(key.getBytes(), null, 0, getGrpcTimeout()).get();
        assertEquals(4, grpcLen);
    }

    @Test
    @DisplayName("Bidirectional: LRANGE in Redis → getList in gRPC")
    void testLRangeBidirectional() throws Exception {
        String key = uniqueKey("lrange");

        jedis.rpush(key, "0", "1", "2", "3", "4");

        // LRANGE with range [1, 3]
        List<String> redisRange = jedis.lrange(key, 1, 3);
        assertEquals(3, redisRange.size());
        assertEquals("1", redisRange.get(0));

        // Get full list via gRPC and verify subset
        List<Variant> grpcList = client.getList(key.getBytes(), null, 0, getGrpcTimeout()).get();
        assertEquals(1, grpcList.get(1).longValue());
        assertEquals(3, grpcList.get(3).longValue());
    }

    @Test
    @DisplayName("Bidirectional: LINDEX in Redis → getAt in gRPC")
    void testLIndexBidirectional() throws Exception {
        String key = uniqueKey("lindex");

        jedis.rpush(key, "first", "second", "third");

        // LINDEX
        String indexed = jedis.lindex(key, 1);
        assertEquals("second", indexed);

        // getAt via gRPC
        Variant grpcAt = client.getAt(key.getBytes(), null, 1, 0, getGrpcTimeout()).get();
        assertEquals("second", new String(grpcAt.bytesValue()));

        // Negative index in Redis
        String negativeIndex = jedis.lindex(key, -1);
        assertEquals("third", negativeIndex);
    }

    @Test
    @DisplayName("Bidirectional: LSET in Redis → update in gRPC")
    void testLSetBidirectional() throws Exception {
        String key = uniqueKey("lset");

        jedis.rpush(key, "original1", "original2", "original3");

        // LSET
        jedis.lset(key, 1, "updated2");

        // Verify via gRPC
        List<Variant> grpcList = client.getList(key.getBytes(), null, 0, getGrpcTimeout()).get();
        assertEquals("original1", new String(grpcList.get(0).bytesValue()));
        assertEquals("updated2", new String(grpcList.get(1).bytesValue()));
        assertEquals("original3", new String(grpcList.get(2).bytesValue()));
    }

    @Test
    @DisplayName("Bidirectional: LREM in Redis → removeFromContainerByKeyAndValue in gRPC")
    void testLRemBidirectional() throws Exception {
        String key = uniqueKey("lrem");

        jedis.rpush(key, "a", "b", "a", "c", "a");

        // LREM - remove all "a" → остаются "b" и "c"
        jedis.lrem(key, 0, "a");

        // Verify via gRPC
        List<Variant> grpcList = client.getList(key.getBytes(), null, 0, getGrpcTimeout()).get();
        assertEquals(2, grpcList.size());
        assertEquals("b", new String(grpcList.get(0).bytesValue()));
        assertEquals("c", new String(grpcList.get(1).bytesValue()));
    }

    @Test
    @DisplayName("Bidirectional: LINSERT in Redis → insertBefore/insertAfter in gRPC")
    void testLInsertBidirectional() throws Exception {
        String key = uniqueKey("linsert");

        jedis.rpush(key, "a", "c");

        // LINSERT BEFORE
        jedis.linsert(key.getBytes(), redis.clients.jedis.args.ListPosition.BEFORE, "c".getBytes(), "b".getBytes());

        // Verify via gRPC
        List<Variant> grpcList = client.getList(key.getBytes(), null, 0, getGrpcTimeout()).get();
        assertEquals(3, grpcList.size());
        assertEquals("a", new String(grpcList.get(0).bytesValue()));
        assertEquals("b", new String(grpcList.get(1).bytesValue()));
        assertEquals("c", new String(grpcList.get(2).bytesValue()));
    }

    @Test
    @DisplayName("Bidirectional: LTRIM in Redis → getAndDeleteListElementsInRange in gRPC")
    void testLTrimBidirectional() throws Exception {
        String key = uniqueKey("ltrim");

        jedis.rpush(key, "0", "1", "2", "3", "4");

        // LTRIM - keep only [1, 3]
        jedis.ltrim(key, 1, 3);

        // Verify via gRPC
        List<Variant> grpcList = client.getList(key.getBytes(), null, 0, getGrpcTimeout()).get();
        assertEquals(3, grpcList.size());
        assertEquals(1, grpcList.get(0).longValue());
        assertEquals(3, grpcList.get(2).longValue());
    }

    @Test
    @DisplayName("Bidirectional: Create list via gRPC, pop via Redis")
    void testListCreatePopBidirectional() throws Exception {
        String key = uniqueKey("listpop");

        // Create list via gRPC
        List<Variant> grpcData = Arrays.asList(toVariant("x1"), toVariant("x2"), toVariant("x3"));
        client.createVector(key.getBytes(), null, grpcData, getContainerTtl(), 0, getGrpcTimeout()).get();

        // Pop from head via Redis
        String redisPopped = jedis.lpop(key);
        assertEquals("x1", redisPopped);

        // Verify via gRPC
        List<Variant> grpcList = client.getList(key.getBytes(), null, 0, getGrpcTimeout()).get();
        assertEquals(2, grpcList.size());
    }

    @Test
    @DisplayName("Bidirectional: Multiple push operations")
    void testMultiplePushOperations() throws Exception {
        String key = uniqueKey("listmulti");

        // Multiple LPUSH
        jedis.lpush(key, "1");
        jedis.lpush(key, "2");
        jedis.lpush(key, "3");

        // Multiple RPUSH
        jedis.rpush(key, "0");
        jedis.rpush(key, "-1");

        // Verify order via gRPC
        List<Variant> grpcList = client.getList(key.getBytes(), null, 0, getGrpcTimeout()).get();
        assertEquals(5, grpcList.size());
        assertEquals(3, grpcList.get(0).longValue());
        assertEquals(-1, grpcList.get(4).longValue());
    }

    @Test
    @DisplayName("Bidirectional: List with duplicate values")
    void testListWithDuplicates() throws Exception {
        String key = uniqueKey("listdup");

        jedis.rpush(key, "a", "a", "b", "a", "b");

        List<String> redisList = jedis.lrange(key, 0, -1);
        assertEquals(5, redisList.size());
        assertEquals(3, Collections.frequency(redisList, "a"));

        List<Variant> grpcList = client.getList(key.getBytes(), null, 0, getGrpcTimeout()).get();
        assertEquals(5, grpcList.size());
    }

    @Test
    @DisplayName("Bidirectional: Delete list container")
    void testListContainerDelete() throws Exception {
        String key = uniqueKey("listdel");

        jedis.rpush(key, "item1", "item2");

        // Delete via gRPC
        client.remove(key.getBytes(), null, 0, getGrpcTimeout()).get();

        // Verify in Redis
        assertNull(jedis.lindex(key, 0));
    }

    @Test
    @DisplayName("Bidirectional: Empty list")
    void testEmptyList() throws Exception {
        String key = uniqueKey("listempty");

        // Create empty list via gRPC
        List<Variant> emptyList = new ArrayList<>();
        client.createVector(key.getBytes(), null, emptyList, getContainerTtl(), 0, getGrpcTimeout()).get();

        // Check in Redis
        long len = jedis.llen(key);
        assertEquals(0, len);
    }

    @Test
    @DisplayName("Bidirectional: Large list")
    void testLargeList() throws Exception {
        String key = uniqueKey("listlarge");

        // Create large list via Redis
        List<String> values = new ArrayList<>();
        for (int i = 0; i < 1000; i++) {
            values.add("value" + i);
        }
        jedis.rpush(key, values.toArray(new String[0]));

        // Verify via gRPC
        List<Variant> grpcList = client.getList(key.getBytes(), null, 0, getGrpcTimeout()).get();
        assertEquals(1000, grpcList.size());
        assertEquals("value0", new String(grpcList.get(0).bytesValue()));
        assertEquals("value999", new String(grpcList.get(999).bytesValue()));
    }

    @Test
    @DisplayName("Bidirectional: List with binary data")
    void testListBinaryData() throws Exception {
        String key = uniqueKey("listbin");

        byte[] data1 = new byte[]{0x01, 0x02, 0x03};
        byte[] data2 = new byte[]{0x10, 0x20, 0x30};

        jedis.rpush(key.getBytes(), data1, data2);

        List<Variant> grpcList = client.getList(key.getBytes(), null, 0, getGrpcTimeout()).get();
        assertArrayEquals(data1, grpcList.get(0).bytesValue());
        assertArrayEquals(data2, grpcList.get(1).bytesValue());
    }

    @Test
    @DisplayName("Bidirectional: Pop head/tail via gRPC")
    void testPopHeadTailGrpc() throws Exception {
        String key = uniqueKey("listpopgrpc");

        // Create via Redis
        jedis.rpush(key, "first", "middle", "last");

        // Pop head via gRPC
        Variant headPopped = client.popHead(key.getBytes(), null, 0, getGrpcTimeout()).get();
        assertEquals("first", new String(headPopped.bytesValue()));

        // Pop tail via gRPC
        Variant tailPopped = client.popTail(key.getBytes(), null, 0, getGrpcTimeout()).get();
        assertEquals("last", new String(tailPopped.bytesValue()));

        // Verify remaining
        List<Variant> grpcList = client.getList(key.getBytes(), null, 0, getGrpcTimeout()).get();
        assertEquals(1, grpcList.size());
        assertEquals("middle", new String(grpcList.get(0).bytesValue()));
    }

    @Test
    @DisplayName("Bidirectional: Push head/tail via gRPC")
    void testPushHeadTailGrpc() throws Exception {
        String key = uniqueKey("listpushgrpc");

        // Create via Redis
        jedis.rpush(key, "middle");

        // Push head via gRPC
        client.pushHead(key.getBytes(), null, Arrays.asList(toVariant("head")), 0, getGrpcTimeout()).get();

        // Push tail via gRPC
        client.pushTail(key.getBytes(), null, Arrays.asList(toVariant("tail")), 0, getGrpcTimeout()).get();

        // Verify via Redis
        List<String> redisList = jedis.lrange(key, 0, -1);
        assertEquals(3, redisList.size());
        assertEquals("head", redisList.get(0));
        assertEquals("middle", redisList.get(1));
        assertEquals("tail", redisList.get(2));
    }
}