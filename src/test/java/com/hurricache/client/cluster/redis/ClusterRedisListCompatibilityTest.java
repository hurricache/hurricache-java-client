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
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import redis.clients.jedis.args.ListDirection;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * Cluster tests for list operations compatibility between Redis and HurriCache.
 * Tests LPUSH, RPUSH, LPOP, RPOP, LLEN, LRANGE, LINDEX.
 * Uses FastCacheAsyncSmartClient for cluster mode testing.
 */
public class ClusterRedisListCompatibilityTest extends RedisCompatibilityTestBase {

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
    @DisplayName("Cluster: LPUSH/RPUSH in Redis → getList in gRPC with cluster key tags")
    void testLPushRPushGetListWithClusterTags() throws Exception {
        String key = uniqueKeyWithTag("cluster_list");

        // LPUSH/RPUSH via Redis
        // HurriCache adds elements in order: first arg first, last arg last
        // Redis lpush reverses: last arg first
        // To get same order in HurriCache, reverse the args
        jedis.lpush(key, "head1", "head2", "head3");
        jedis.rpush(key, "tail1", "tail2");

        // List order: head3, head2, head1, tail1, tail2 (Redis lpush reverses)
        List<String> redisList = jedis.lrange(key, 0, -1);
        assertEquals(5, redisList.size());
        assertEquals("head3", redisList.get(0));
        awaitReplication(); // Wait for replication
        // Read via gRPC — HurriCache keeps insertion order
        // After Redis lpush("head1", "head2", "head3") the list is [head3, head2, head1]
        // After Redis rpush("tail1", "tail2") the list is [head3, head2, head1, tail2, tail1]
        List<Variant> grpcList = client.getList(key.getBytes(), null, 0, getGrpcTimeout()).get();
        assertEquals(5, grpcList.size());
        assertEquals("head3", new String(grpcList.get(0).bytesValue()));
        assertEquals("tail2", new String(grpcList.get(4).bytesValue()));
    }

    @Test
    @DisplayName("Cluster: CREATE LIST in gRPC → LRANGE in Redis")
    void testCreateListLRangeBidirectional() throws Exception {
        String key = uniqueKeyWithTag("cluster_create_list");

        // CREATE LIST via gRPC
        List<Variant> grpcData = Arrays.asList(
            toVariant("g1"), toVariant("g2"), toVariant("g3")
        );
        client.createVector(key, null, grpcData, getContainerTtl(), 0, getGrpcTimeout()).get();
        Thread.sleep(500); // replication delay
        // Read via Redis
        List<String> redisFromGrpc = jedis.lrange(key, 0, -1);
        assertEquals(3, redisFromGrpc.size());
        assertEquals("g1", redisFromGrpc.get(0));
        assertEquals("g3", redisFromGrpc.get(2));
    }

    @Test
    @DisplayName("Cluster: LPUSH + LPOP in Redis → getList + popHead in gRPC")
    void testLPopRPopGetListBidirectional() throws Exception {
        String key = uniqueKeyWithTag("cluster_lpop");

        // Create list via Redis
        jedis.rpush(key, "item1", "item2", "item3");
        Thread.sleep(500); // replication delay
        // LPOP via Redis
        String popped = jedis.lpop(key);
        assertEquals("item1", popped);
        awaitReplication();
        // Verify via gRPC
        List<Variant> grpcList = client.getList(key, null, 0, getGrpcTimeout()).get();
        assertEquals(2, grpcList.size());
        assertEquals("item2", new String(grpcList.get(0).bytesValue()));

        // RPOP via Redis
        String rPopped = jedis.rpop(key);
        assertEquals("item3", rPopped);
        awaitReplication();
        grpcList = client.getList(key.getBytes(), null, 0, getGrpcTimeout()).get();
        assertEquals(1, grpcList.size());
        assertEquals("item2", new String(grpcList.get(0).bytesValue()));
    }

    @Test
    @DisplayName("Cluster: LLEN in Redis → getSize in gRPC")
    void testLLenGetSizeBidirectional() throws Exception {
        String key = uniqueKeyWithTag("cluster_llen");

        jedis.rpush(key, "a", "b", "c", "d");
        Thread.sleep(500); // replication delay
        long redisLen = jedis.llen(key);
        assertEquals(4L, redisLen);

        int grpcLen = client.getSize(key.getBytes(), null, 0, getGrpcTimeout()).get();
        assertEquals(4, grpcLen);
    }

    @Test
    @DisplayName("Cluster: LRANGE in Redis → getList in gRPC")
    void testLRangeGetListBidirectional() throws Exception {
        String key = uniqueKeyWithTag("cluster_lrange");

        jedis.rpush(key, "0", "1", "2", "3", "4");
        Thread.sleep(500); // replication delay
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
    @DisplayName("Cluster: LINDEX in Redis → getAt in gRPC")
    void testLIndexGetAtBidirectional() throws Exception {
        String key = uniqueKeyWithTag("cluster_lindex");

        jedis.rpush(key, "first", "second", "third");
        awaitReplication();
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
    @DisplayName("Cluster: LSET in Redis → update in gRPC")
    void testLSetUpdateBidirectional() throws Exception {
        String key = uniqueKeyWithTag("cluster_lset");

        jedis.rpush(key, "original1", "original2", "original3");

        // LSET
        jedis.lset(key, 1, "updated2");
        awaitReplication(); // Wait for replication
        // Verify via gRPC
        List<Variant> grpcList = client.getList(key.getBytes(), null, 0, getGrpcTimeout()).get();
        assertEquals("original1", new String(grpcList.get(0).bytesValue()));
        assertEquals("updated2", new String(grpcList.get(1).bytesValue()));
        assertEquals("original3", new String(grpcList.get(2).bytesValue()));
    }

    @Test
    @DisplayName("Cluster: LREM in Redis → remove elements in gRPC")
    void testLRemRemoveBidirectional() throws Exception {
        String key = uniqueKeyWithTag("cluster_lrem");

        jedis.rpush(key, "a", "b", "a", "c", "a");
        Thread.sleep(500); // replication delay
        // LREM - remove all "a" → остаются "b" и "c"
        jedis.lrem(key, 0, "a");
        Thread.sleep(500); // replication delay
        // Verify via gRPC
        List<Variant> grpcList = client.getList(key.getBytes(), null, 0, getGrpcTimeout()).get();
        assertEquals(2, grpcList.size());
        assertEquals("b", new String(grpcList.get(0).bytesValue()));
        assertEquals("c", new String(grpcList.get(1).bytesValue()));
    }

    @Test
    @DisplayName("Cluster: LINSERT in Redis → insert in gRPC")
    void testLInsertInsertBidirectional() throws Exception {
        String key = uniqueKeyWithTag("cluster_linsert");

        jedis.rpush(key, "a", "c");

        // LINSERT BEFORE
        jedis.linsert(key.getBytes(), redis.clients.jedis.args.ListPosition.BEFORE, "c".getBytes(), "b".getBytes());
        awaitReplication(); // jedis-чтение может попасть на бэкап
        // Verify via gRPC
        List<Variant> grpcList = client.getList(key.getBytes(), null, 0, getGrpcTimeout()).get();
        assertEquals(3, grpcList.size());
        assertEquals("a", new String(grpcList.get(0).bytesValue()));
        assertEquals("b", new String(grpcList.get(1).bytesValue()));
        assertEquals("c", new String(grpcList.get(2).bytesValue()));
    }

    @Test
    @DisplayName("Cluster: LTRIM in Redis → remove range in gRPC")
    void testLTrimRemoveRangeBidirectional() throws Exception {
        String key = uniqueKeyWithTag("cluster_ltrim");

        jedis.rpush(key, "0", "1", "2", "3", "4");
        Thread.sleep(500); // replication delay
        // LTRIM - keep only [1, 3]
        jedis.ltrim(key, 1, 3);
        Thread.sleep(500); // replication delay
        // Verify via gRPC
        List<Variant> grpcList = client.getList(key.getBytes(), null, 0, getGrpcTimeout()).get();
        assertEquals(3, grpcList.size());
        assertEquals(1, grpcList.get(0).longValue());
        assertEquals(3, grpcList.get(2).longValue());
    }

    @Test
    @DisplayName("Cluster: Create list via gRPC, pop via Redis")
    void testListCreatePopBidirectional() throws Exception {
        String key = uniqueKeyWithTag("cluster_listpop");

        // Create list via gRPC
        List<Variant> grpcData = Arrays.asList(toVariant("x1"), toVariant("x2"), toVariant("x3"));
        client.createVector(key.getBytes(), null, grpcData, getContainerTtl(), 0, getGrpcTimeout()).get();
        awaitReplication(); // Wait for replication before Redis pop
        // Pop from head via Redis
        String redisPopped = jedis.lpop(key);
        assertEquals("x1", redisPopped);
        awaitReplication();
        // Verify via gRPC
        List<Variant> grpcList = client.getList(key.getBytes(), null, 0, getGrpcTimeout()).get();
        assertEquals(2, grpcList.size());
    }

    @Test
    @DisplayName("Cluster: Multiple push operations")
    void testMultiplePushOperationsInCluster() throws Exception {
        String key = uniqueKeyWithTag("cluster_listmulti");

        // Multiple LPUSH
        jedis.lpush(key, "1");
        jedis.lpush(key, "2");
        jedis.lpush(key, "3");

        // Multiple RPUSH
        jedis.rpush(key, "0");
        jedis.rpush(key, "-1");
        awaitReplication(); // Wait for replication before gRPC read
        // Verify order via gRPC
        // After lpush("1"): [1]
        // After lpush("2"): [2, 1]
        // After lpush("3"): [3, 2, 1]
        // After rpush("0"): [3, 2, 1, 0]
        // After rpush("-1"): [3, 2, 1, 0, -1]
        List<Variant> grpcList = client.getList(key.getBytes(), null, 0, getGrpcTimeout()).get();
        assertEquals(5, grpcList.size());
        assertEquals(3, grpcList.get(0).longValue());
        assertEquals(-1, grpcList.get(4).longValue());
    }

    @Test
    @DisplayName("Cluster: List with duplicate values")
    void testListWithDuplicatesInCluster() throws Exception {
        String key = uniqueKeyWithTag("cluster_listdup");

        jedis.rpush(key, "a", "a", "b", "a", "b");
        Thread.sleep(500); // replication delay
        List<String> redisList = jedis.lrange(key, 0, -1);
        assertEquals(5, redisList.size());
        assertEquals(3, Collections.frequency(redisList, "a"));
        Thread.sleep(500); // replication delay
        List<Variant> grpcList = client.getList(key.getBytes(), null, 0, getGrpcTimeout()).get();
        assertEquals(5, grpcList.size());
    }

    @Test
    @DisplayName("Cluster: Delete list container")
    void testListContainerDeleteInCluster() throws Exception {
        String key = uniqueKeyWithTag("cluster_listdel");

        jedis.rpush(key, "item1", "item2");
        Thread.sleep(500); // replication delay
        // Delete via gRPC
        client.remove(key.getBytes(), null, 0, getGrpcTimeout()).get();
        Thread.sleep(500); // replication delay
        // Verify in Redis
        assertNull(jedis.lindex(key, 0));
    }

    @Test
    @DisplayName("Cluster: Empty list")
    void testEmptyListInCluster() throws Exception {
        String key = uniqueKeyWithTag("cluster_listempty");

        // Create empty list via gRPC
        List<Variant> emptyList = new ArrayList<>();
        client.createVector(key.getBytes(), null, emptyList, getContainerTtl(), 0, getGrpcTimeout()).get();

        // Check in Redis
        long len = jedis.llen(key);
        assertEquals(0, len);
    }

    @Test
    @DisplayName("Cluster: Large list")
    void testLargeListInCluster() throws Exception {
        String key = uniqueKeyWithTag("cluster_listlarge");

        // Create large list via Redis
        List<String> values = new ArrayList<>();
        for (int i = 0; i < 1000; i++) {
            values.add("value" + i);
        }
        jedis.rpush(key, values.toArray(new String[0]));
        Thread.sleep(500); // replication delay
        // Verify via gRPC
        List<Variant> grpcList = client.getList(key, null, 0, getGrpcTimeout()).get();
        assertEquals(1000, grpcList.size());
        assertEquals("value0", new String(grpcList.get(0).bytesValue()));
        assertEquals("value999", new String(grpcList.get(999).bytesValue()));
    }

    @Test
    @DisplayName("Cluster: List with binary data")
    void testListBinaryDataInCluster() throws Exception {
        String key = uniqueKeyWithTag("cluster_listbin");

        byte[] data1 = new byte[]{0x01, 0x02, 0x03};
        byte[] data2 = new byte[]{0x10, 0x20, 0x30};

        jedis.rpush(key.getBytes(), data1, data2);
        Thread.sleep(500); // replication delay
        List<Variant> grpcList = client.getList(key.getBytes(), null, 0, getGrpcTimeout()).get();
        assertArrayEquals(data1, grpcList.get(0).bytesValue());
        assertArrayEquals(data2, grpcList.get(1).bytesValue());
    }

    @Test
    @DisplayName("Cluster: Pop head/tail via gRPC")
    void testPopHeadTailGrpcInCluster() throws Exception {
        String key = uniqueKeyWithTag("cluster_listpopgrpc");

        // Create via Redis
        jedis.rpush(key, "first", "middle", "last");
        Thread.sleep(500); // replication delay
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
    @DisplayName("Cluster: Push head/tail via gRPC")
    void testPushHeadTailGrpcInCluster() throws Exception {
        String key = uniqueKeyWithTag("cluster_listpushgrpc");

        // Create via Redis
        jedis.rpush(key, "middle");
        Thread.sleep(500); // replication delay
        // Push head via gRPC
        client.pushHead(key.getBytes(), null, Arrays.asList(toVariant("head")), 0, getGrpcTimeout()).get();

        // Push tail via gRPC
        client.pushTail(key.getBytes(), null, Arrays.asList(toVariant("tail")), 0, getGrpcTimeout()).get();
        Thread.sleep(500); // replication delay
        // Verify via Redis
        List<String> redisList = jedis.lrange(key, 0, -1);
        assertEquals(3, redisList.size());
        assertEquals("head", redisList.get(0));
        assertEquals("middle", redisList.get(1));
        assertEquals("tail", redisList.get(2));
    }

    @Test
    @DisplayName("Cluster: List with unicode values")
    void testListUnicodeInCluster() throws Exception {
        String key = uniqueKeyWithTag("cluster_listuni");

        jedis.rpush(key, "\u041F\u0440\u0438\u0432\u0435\u0442", "\u4F60\u597D", "\uD83C\uDF0D");
        Thread.sleep(500); // replication delay
        List<Variant> grpcList = client.getList(key.getBytes(), null, 0, getGrpcTimeout()).get();
        assertEquals(3, grpcList.size());
        assertEquals("\u041F\u0440\u0438\u0432\u0435\u0442", new String(grpcList.get(0).bytesValue()));
        assertEquals("\u4F60\u597D", new String(grpcList.get(1).bytesValue()));
        assertEquals("\uD83C\uDF0D", new String(grpcList.get(2).bytesValue()));
    }

    @Test
    @DisplayName("Cluster: Multiple list operations stress")
    void testMultipleListOpsInCluster() throws Exception {
        String baseKey = uniqueKeyWithTag("cluster_listmulti");

        // Create multiple list containers
        for (int i = 0; i < 10; i++) {
            String key = baseKey + ":" + i;
            jedis.rpush(key, "item1", "item2", "item3");
        }
        Thread.sleep(500); // replication delay
        // Read all via gRPC
        for (int i = 0; i < 10; i++) {
            String key = baseKey + ":" + i;
            List<Variant> grpcList = client.getList(key.getBytes(), null, 0, getGrpcTimeout()).get();
            assertEquals(3, grpcList.size());
            assertEquals("item1", new String(grpcList.get(0).bytesValue()));
        }
    }

    @Test
    @DisplayName("Cluster: LPOS in Redis")
    void testLPosInCluster() throws Exception {
        String key = uniqueKeyWithTag("cluster_lpos");

        jedis.rpush(key, "a", "b", "c", "b");
        awaitReplication();

        assertEquals(1L, jedis.lpos(key, "b"));
        assertNull(jedis.lpos(key, "missing"));
    }

    @Test
    @DisplayName("Cluster: LMOVE in Redis → gRPC")
    void testLMoveInCluster() throws Exception {
        String src = uniqueKeyWithTag("cluster_lmove_src");
        String dst = uniqueKeyWithTag("cluster_lmove_dst");

        jedis.rpush(src, "1", "2", "3");
        jedis.rpush(dst, "x");

        awaitReplication();
        // RIGHT → LEFT: "3" со src-хвоста в голову dst
        String moved = jedis.lmove(src, dst, ListDirection.RIGHT, ListDirection.LEFT);
        assertEquals("3", moved);

        awaitReplication();
        List<Variant> srcList = client.getList(src.getBytes(), null, 0, getGrpcTimeout()).get();
        assertEquals(2, srcList.size());
        List<Variant> dstList = client.getList(dst.getBytes(), null, 0, getGrpcTimeout()).get();
        assertEquals(2, dstList.size());
        assertEquals(3, dstList.get(0).longValue());
    }

    @Test
    @DisplayName("Cluster: RPOPLPUSH in Redis → gRPC")
    void testRpopLPushInCluster() throws Exception {
        String src = uniqueKeyWithTag("cluster_rpoplpush_src");
        String dst = uniqueKeyWithTag("cluster_rpoplpush_dst");

        jedis.rpush(src, "a", "b", "c");

        awaitReplication();
        String moved = jedis.rpoplpush(src, dst);
        assertEquals("c", moved);

        awaitReplication();
        List<Variant> srcList = client.getList(src.getBytes(), null, 0, getGrpcTimeout()).get();
        assertEquals(2, srcList.size());
        List<Variant> dstList = client.getList(dst.getBytes(), null, 0, getGrpcTimeout()).get();
        assertEquals(1, dstList.size());
        assertEquals("c", new String(dstList.get(0).bytesValue()));
    }

    @Test
    @DisplayName("Cluster: BLPOP/BRPOP on empty list return nil immediately")
    void testBlPopBrPopEmptyInCluster() throws Exception {
        String key = uniqueKeyWithTag("cluster_blpop_empty");

        // Неблокирующая реализация: на пустом списке сразу nil
        assertNull(jedis.blpop(1, key));
        assertNull(jedis.brpop(1, key));
    }

    @Test
    @DisplayName("Cluster: BLPOP on non-empty list in Redis → gRPC")
    void testBlPopNonEmptyInCluster() throws Exception {
        String key = uniqueKeyWithTag("cluster_blpop");

        jedis.rpush(key, "first", "second");
        awaitReplication();

        List<String> popped = jedis.blpop(1, key);
        assertNotNull(popped);
        assertEquals(key, popped.get(0));
        assertEquals("first", popped.get(1));

        awaitReplication();
        List<Variant> grpcList = client.getList(key.getBytes(), null, 0, getGrpcTimeout()).get();
        assertEquals(1, grpcList.size());
        assertEquals("second", new String(grpcList.get(0).bytesValue()));
    }
}