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
import com.hurricache.client.intf.Variant;
import com.hurricache.redis.RedisCompatibilityTestBase;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Проверка репликации RESP-команд: запись через jedis (RESP), чтение через gRPC.
 * Покрывает свежую репликацию: sets/zsets/lists/hashes/скаляры.
 * Standalone режим (FastCacheAsyncSimpleClient → 127.0.0.1:50000).
 */
public class StandaloneRedisReplicationCompatibilityTest extends RedisCompatibilityTestBase {

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
        return "Standalone replication (FastCacheAsyncSimpleClient)";
    }

    // =========================================================================
    // SETS: SADD / SREM / SPOP
    // =========================================================================

    @Test
    @DisplayName("Replication: SADD+SREM via Redis → gRPC")
    void testSetAddRemoveReplication() throws Exception {
        String key = uniqueKey("repl_set");

        jedis.sadd(key, "a", "b", "c", "d");
        jedis.srem(key, "b", "d");
        awaitReplication();

        List<Variant> grpcSet = client.getSet(key).get();
        assertEquals(2, grpcSet.size());
    }

    @Test
    @DisplayName("Replication: SPOP via Redis → gRPC")
    void testSetPopReplication() throws Exception {
        String key = uniqueKey("repl_spop");

        jedis.sadd(key, "one", "two", "three");
        jedis.spop(key);
        awaitReplication();

        assertEquals(2, client.getSet(key).get().size());
    }

    // =========================================================================
    // ZSETS: ZADD / weight update / ZREM / ZINCRBY / ZPOPMIN
    // =========================================================================

    @Test
    @DisplayName("Replication: ZADD + ZINCRBY via Redis → gRPC")
    void testZSetIncrReplication() throws Exception {
        String key = uniqueKey("repl_zincr");

        jedis.zadd(key, 10.0, "counter");
        jedis.zincrby(key, 5.0, "counter");
        awaitReplication();

        var grpcSet = client.getOrderedSet(key).get();
        assertEquals(1, grpcSet.size());
        assertEquals(15L, grpcSet.get(0).getOrder());
    }

    @Test
    @DisplayName("Replication: ZADD weight update via Redis → gRPC")
    void testZSetWeightUpdateReplication() throws Exception {
        String key = uniqueKey("repl_zupdate");

        jedis.zadd(key, 10.0, "member");
        jedis.zadd(key, 20.0, "member"); // HurriCache: дубликаты по весу допустимы
        jedis.zrem(key, "member");       // удаляет по значению — оба
        awaitReplication();

        assertEquals(0, client.getOrderedSet(key).get().size());
    }

    @Test
    @DisplayName("Replication: ZPOPMIN via Redis → gRPC")
    void testZPopMinReplication() throws Exception {
        String key = uniqueKey("repl_zpopmin");

        jedis.zadd(key, 1.0, "lowest");
        jedis.zadd(key, 2.0, "middle");
        jedis.zadd(key, 3.0, "highest");
        jedis.zpopmin(key);
        awaitReplication();

        assertEquals(2, client.getOrderedSet(key).get().size());
    }

    // =========================================================================
    // LISTS: LPUSH (порядок!) / LPOP / LSET / LTRIM / LINSERT
    // =========================================================================

    @Test
    @DisplayName("Replication: LPUSH order via Redis → gRPC")
    void testListPushOrderReplication() throws Exception {
        String key = uniqueKey("repl_lpush");

        // Redis LPUSH: каждый аргумент в голову слева направо → [c, b, a]
        jedis.lpush(key, "a", "b", "c");
        awaitReplication();

        List<Variant> grpcList = client.getList(key).get();
        assertEquals(3, grpcList.size());
        assertEquals("c", new String(grpcList.get(0).bytesValue(), StandardCharsets.UTF_8));
        assertEquals("a", new String(grpcList.get(2).bytesValue(), StandardCharsets.UTF_8));
    }

    @Test
    @DisplayName("Replication: LPOP via Redis → gRPC")
    void testListPopReplication() throws Exception {
        String key = uniqueKey("repl_lpop");

        jedis.rpush(key, "x1", "x2", "x3");
        jedis.lpop(key);
        awaitReplication();

        List<Variant> grpcList = client.getList(key).get();
        assertEquals(2, grpcList.size());
        assertEquals("x2", new String(grpcList.get(0).bytesValue(), StandardCharsets.UTF_8));
    }

    @Test
    @DisplayName("Replication: LSET via Redis → gRPC")
    void testListSetReplication() throws Exception {
        String key = uniqueKey("repl_lset");

        jedis.rpush(key, "v0", "v1", "v2");
        jedis.lset(key, 1, "replaced");
        awaitReplication();

        assertEquals("replaced", new String(client.getList(key).get().get(1).bytesValue(), StandardCharsets.UTF_8));
    }

    @Test
    @DisplayName("Replication: LTRIM via Redis → gRPC")
    void testListTrimReplication() throws Exception {
        String key = uniqueKey("repl_ltrim");

        jedis.rpush(key, "0", "1", "2", "3", "4");
        jedis.ltrim(key, 1, 3);
        awaitReplication();

        List<Variant> grpcList = client.getList(key).get();
        assertEquals(3, grpcList.size());
        assertEquals(1, grpcList.get(0).longValue());
        assertEquals(3, grpcList.get(2).longValue());
    }

    @Test
    @DisplayName("Replication: LINSERT via Redis → gRPC")
    void testListInsertReplication() throws Exception {
        String key = uniqueKey("repl_linsert");

        jedis.rpush(key, "a", "c");
        jedis.linsert(key, redis.clients.jedis.args.ListPosition.BEFORE, "c", "b");
        awaitReplication();

        List<Variant> grpcList = client.getList(key).get();
        assertEquals(3, grpcList.size());
        assertEquals("b", new String(grpcList.get(1).bytesValue(), StandardCharsets.UTF_8));
    }

    // =========================================================================
    // HASHES: HSET (новое поле + замена) / HDEL / HINCRBY / HINCRBYFLOAT
    // =========================================================================

    @Test
    @DisplayName("Replication: HSET add + replace via Redis → gRPC")
    void testHashSetReplication() throws Exception {
        String key = uniqueKey("repl_hset");

        jedis.hset(key, "f1", "v1");
        jedis.hset(key, "f2", "v2");
        jedis.hset(key, "f1", "v1_updated");
        awaitReplication();

        assertEquals(2, client.getMap(key).get().size());
    }

    @Test
    @DisplayName("Replication: HDEL via Redis → gRPC")
    void testHashDelReplication() throws Exception {
        String key = uniqueKey("repl_hdel");

        jedis.hset(key, "f1", "v1");
        jedis.hset(key, "f2", "v2");
        jedis.hdel(key, "f1");
        awaitReplication();

        assertEquals(1, client.getMap(key).get().size());
    }

    @Test
    @DisplayName("Replication: HINCRBY via Redis → typed INT on gRPC")
    void testHashIncrByReplication() throws Exception {
        String key = uniqueKey("repl_hincrby");

        jedis.hincrBy(key, "counter", 10);
        jedis.hincrBy(key, "counter", 5);
        awaitReplication();

        Variant val = client.getInContainer(key, "counter".getBytes(StandardCharsets.UTF_8)).get();
        assertEquals(15, val.longValue());
    }

    @Test
    @DisplayName("Replication: HINCRBYFLOAT via Redis → typed DOUBLE on gRPC")
    void testHashIncrByFloatReplication() throws Exception {
        String key = uniqueKey("repl_hincrbyfloat");

        jedis.hincrByFloat(key, "score", 1.5);
        jedis.hincrByFloat(key, "score", 0.25);
        awaitReplication();

        Variant val = client.getInContainer(key, "score".getBytes(StandardCharsets.UTF_8)).get();
        assertEquals(1.75, val.doubleValue(), 0.0001);
    }

    // =========================================================================
    // SCALARS: MSET / EXPIRE / GETDEL
    // =========================================================================

    @Test
    @DisplayName("Replication: MSET via Redis → gRPC")
    void testMSetReplication() throws Exception {
        String key1 = uniqueKey("repl_mset1");
        String key2 = uniqueKey("repl_mset2");

        jedis.mset(key1, "mv1", key2, "mv2");
        awaitReplication();

        assertEqualsVariantString(client.get(key1).get(), "mv1");
        assertEqualsVariantString(client.get(key2).get(), "mv2");
    }

    @Test
    @DisplayName("Replication: EXPIRE via Redis → TTL on gRPC")
    void testExpireReplication() throws Exception {
        String key = uniqueKey("repl_expire");

        jedis.set(key, "expiring");
        jedis.expire(key, 600);
        awaitReplication();

        Long ttl = client.getTtl(key).get();
        assertNotNull(ttl);
        assertTrue(ttl > 0);
    }

    @Test
    @DisplayName("Replication: GETDEL via Redis → absent on gRPC")
    void testGetDelReplication() throws Exception {
        String key = uniqueKey("repl_getdel");

        jedis.set(key, "to_delete");
        assertEquals("to_delete", jedis.getDel(key));
        awaitReplication();

        assertFalse(client.containsKey(key).get());
    }
}
