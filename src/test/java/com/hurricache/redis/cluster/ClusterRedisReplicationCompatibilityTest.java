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
import com.hurricache.client.intf.Mode;
import com.hurricache.client.intf.Variant;
import com.hurricache.redis.RedisCompatibilityTestBase;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Проверка репликации RESP-команд: запись через jedis (RESP), чтение через gRPC
 * сначала с MASTER, потом с BACKUP (после awaitReplication).
 * Покрывает свежую репликацию: sets/zsets/lists/hashes/скаляры.
 */
public class ClusterRedisReplicationCompatibilityTest extends RedisCompatibilityTestBase {

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
        return "Cluster replication (FastCacheAsyncSmartClient)";
    }

    // setMode живёт на SmartClient, а не на интерфейсе
    private FastCacheAsyncSmartClient smart() {
        return (FastCacheAsyncSmartClient) client;
    }

    // =========================================================================
    // SETS: SADD / SREM / SPOP
    // =========================================================================

    @Test
    @DisplayName("Replication: SADD+SREM via Redis → BACKUP via gRPC")
    void testSetAddRemoveReplication() throws Exception {
        String key = uniqueKeyWithTag("repl_set");

        jedis.sadd(key, "a", "b", "c", "d");
        jedis.srem(key, "b", "d");
        awaitReplication();

        // MASTER
        smart().setMode(Mode.MASTER);
        List<Variant> masterSet = client.getSet(key).get();
        assertEquals(2, masterSet.size());

        // BACKUP
        List<Variant> backupSet = smart().setMode(Mode.BACKUP).getSet(key).get();
        assertEquals(2, backupSet.size());
    }

    @Test
    @DisplayName("Replication: SPOP via Redis → BACKUP via gRPC")
    void testSetPopReplication() throws Exception {
        String key = uniqueKeyWithTag("repl_spop");

        jedis.sadd(key, "one", "two", "three");
        jedis.spop(key);
        awaitReplication();

        smart().setMode(Mode.MASTER);
        assertEquals(2, client.getSet(key).get().size());
        assertEquals(2, smart().setMode(Mode.BACKUP).getSet(key).get().size());
    }

    // =========================================================================
    // ZSETS: ZADD / weight update / ZREM / ZINCRBY / ZPOPMIN
    // =========================================================================

    @Test
    @DisplayName("Replication: ZADD + ZINCRBY via Redis → BACKUP via gRPC")
    void testZSetIncrReplication() throws Exception {
        String key = uniqueKeyWithTag("repl_zincr");

        jedis.zadd(key, 10.0, "counter");
        jedis.zincrby(key, 5.0, "counter");
        awaitReplication();

        smart().setMode(Mode.MASTER);
        var masterSet = client.getOrderedSet(key).get();
        assertEquals(1, masterSet.size());
        assertEquals(15L, masterSet.get(0).getOrder());

        var backupSet = smart().setMode(Mode.BACKUP).getOrderedSet(key).get();
        assertEquals(1, backupSet.size());
        assertEquals(15L, backupSet.get(0).getOrder());
    }

    @Test
    @DisplayName("Replication: ZADD weight update via Redis → BACKUP via gRPC")
    void testZSetWeightUpdateReplication() throws Exception {
        String key = uniqueKeyWithTag("repl_zupdate");

        jedis.zadd(key, 10.0, "member");
        jedis.zadd(key, 20.0, "member"); // HurriCache: дубликаты по весу допустимы
        jedis.zrem(key, "member");       // удаляет по значению — оба
        awaitReplication();

        smart().setMode(Mode.MASTER);
        assertEquals(0, client.getOrderedSet(key).get().size());
        assertEquals(0, smart().setMode(Mode.BACKUP).getOrderedSet(key).get().size());
    }

    @Test
    @DisplayName("Replication: ZPOPMIN via Redis → BACKUP via gRPC")
    void testZPopMinReplication() throws Exception {
        String key = uniqueKeyWithTag("repl_zpopmin");

        jedis.zadd(key, 1.0, "lowest");
        jedis.zadd(key, 2.0, "middle");
        jedis.zadd(key, 3.0, "highest");
        jedis.zpopmin(key);
        awaitReplication();

        smart().setMode(Mode.MASTER);
        assertEquals(2, client.getOrderedSet(key).get().size());
        assertEquals(2, smart().setMode(Mode.BACKUP).getOrderedSet(key).get().size());
    }

    // =========================================================================
    // LISTS: LPUSH (порядок!) / LPOP / LSET / LTRIM / LINSERT
    // =========================================================================

    @Test
    @DisplayName("Replication: LPUSH order via Redis → BACKUP via gRPC")
    void testListPushOrderReplication() throws Exception {
        String key = uniqueKeyWithTag("repl_lpush");

        // Redis LPUSH: каждый аргумент в голову слева направо → [c, b, a]
        jedis.lpush(key, "a", "b", "c");
        awaitReplication();

        smart().setMode(Mode.MASTER);
        List<Variant> masterList = client.getList(key).get();
        assertEquals(3, masterList.size());
        assertEquals("c", new String(masterList.get(0).bytesValue(), StandardCharsets.UTF_8));

        List<Variant> backupList = smart().setMode(Mode.BACKUP).getList(key).get();
        assertEquals(3, backupList.size());
        assertEquals("c", new String(backupList.get(0).bytesValue(), StandardCharsets.UTF_8));
        assertEquals("a", new String(backupList.get(2).bytesValue(), StandardCharsets.UTF_8));
    }

    @Test
    @DisplayName("Replication: LPOP via Redis → BACKUP via gRPC")
    void testListPopReplication() throws Exception {
        String key = uniqueKeyWithTag("repl_lpop");

        jedis.rpush(key, "x1", "x2", "x3");
        jedis.lpop(key);
        awaitReplication();

        smart().setMode(Mode.MASTER);
        List<Variant> masterList = client.getList(key).get();
        assertEquals(2, masterList.size());
        assertEquals("x2", new String(masterList.get(0).bytesValue(), StandardCharsets.UTF_8));

        List<Variant> backupList = smart().setMode(Mode.BACKUP).getList(key).get();
        assertEquals(2, backupList.size());
        assertEquals("x2", new String(backupList.get(0).bytesValue(), StandardCharsets.UTF_8));
    }

    @Test
    @DisplayName("Replication: LSET via Redis → BACKUP via gRPC")
    void testListSetReplication() throws Exception {
        String key = uniqueKeyWithTag("repl_lset");

        jedis.rpush(key, "v0", "v1", "v2");
        jedis.lset(key, 1, "replaced");
        awaitReplication();

        smart().setMode(Mode.MASTER);
        assertEquals("replaced", new String(client.getList(key).get().get(1).bytesValue(), StandardCharsets.UTF_8));
        assertEquals("replaced", new String(smart().setMode(Mode.BACKUP).getList(key).get().get(1).bytesValue(), StandardCharsets.UTF_8));
    }

    @Test
    @DisplayName("Replication: LTRIM via Redis → BACKUP via gRPC")
    void testListTrimReplication() throws Exception {
        String key = uniqueKeyWithTag("repl_ltrim");

        jedis.rpush(key, "0", "1", "2", "3", "4");
        jedis.ltrim(key, 1, 3);
        awaitReplication();

        smart().setMode(Mode.MASTER);
        List<Variant> masterList = client.getList(key).get();
        assertEquals(3, masterList.size());
        assertEquals(1, masterList.get(0).longValue());

        List<Variant> backupList = smart().setMode(Mode.BACKUP).getList(key).get();
        assertEquals(3, backupList.size());
        assertEquals(1, backupList.get(0).longValue());
        assertEquals(3, backupList.get(2).longValue());
    }

    @Test
    @DisplayName("Replication: LINSERT via Redis → BACKUP via gRPC")
    void testListInsertReplication() throws Exception {
        String key = uniqueKeyWithTag("repl_linsert");

        jedis.rpush(key, "a", "c");
        jedis.linsert(key, redis.clients.jedis.args.ListPosition.BEFORE, "c", "b");
        awaitReplication();

        smart().setMode(Mode.MASTER);
        assertEquals(3, client.getList(key).get().size());

        List<Variant> backupList = smart().setMode(Mode.BACKUP).getList(key).get();
        assertEquals(3, backupList.size());
        assertEquals("b", new String(backupList.get(1).bytesValue(), StandardCharsets.UTF_8));
    }

    // =========================================================================
    // HASHES: HSET (новое поле + замена) / HDEL / HINCRBY / HINCRBYFLOAT
    // =========================================================================

    @Test
    @DisplayName("Replication: HSET add + replace via Redis → BACKUP via gRPC")
    void testHashSetReplication() throws Exception {
        String key = uniqueKeyWithTag("repl_hset");

        jedis.hset(key, "f1", "v1");
        jedis.hset(key, "f2", "v2");
        jedis.hset(key, "f1", "v1_updated");
        awaitReplication();

        smart().setMode(Mode.MASTER);
        assertEquals(2, client.getMap(key).get().size());

        var backupMap = smart().setMode(Mode.BACKUP).getMap(key).get();
        assertEquals(2, backupMap.size());
    }

    @Test
    @DisplayName("Replication: HDEL via Redis → BACKUP via gRPC")
    void testHashDelReplication() throws Exception {
        String key = uniqueKeyWithTag("repl_hdel");

        jedis.hset(key, "f1", "v1");
        jedis.hset(key, "f2", "v2");
        jedis.hdel(key, "f1");
        awaitReplication();

        smart().setMode(Mode.MASTER);
        assertEquals(1, client.getMap(key).get().size());
        assertEquals(1, smart().setMode(Mode.BACKUP).getMap(key).get().size());
    }

    @Test
    @DisplayName("Replication: HINCRBY via Redis → typed INT on BACKUP via gRPC")
    void testHashIncrByReplication() throws Exception {
        String key = uniqueKeyWithTag("repl_hincrby");

        jedis.hincrBy(key, "counter", 10);
        jedis.hincrBy(key, "counter", 5);
        awaitReplication();

        smart().setMode(Mode.MASTER);
        Variant masterVal = client.getInContainer(key, "counter".getBytes(StandardCharsets.UTF_8)).get();
        assertEquals(15, masterVal.longValue());

        Variant backupVal = smart().setMode(Mode.BACKUP)
                .getInContainer(key, "counter".getBytes(StandardCharsets.UTF_8)).get();
        assertEquals(15, backupVal.longValue());
    }

    @Test
    @DisplayName("Replication: HINCRBYFLOAT via Redis → typed DOUBLE on BACKUP via gRPC")
    void testHashIncrByFloatReplication() throws Exception {
        String key = uniqueKeyWithTag("repl_hincrbyfloat");

        jedis.hincrByFloat(key, "score", 1.5);
        jedis.hincrByFloat(key, "score", 0.25);
        awaitReplication();

        smart().setMode(Mode.MASTER);
        Variant masterVal = client.getInContainer(key, "score".getBytes(StandardCharsets.UTF_8)).get();
        assertEquals(1.75, masterVal.doubleValue(), 0.0001);

        Variant backupVal = smart().setMode(Mode.BACKUP)
                .getInContainer(key, "score".getBytes(StandardCharsets.UTF_8)).get();
        assertEquals(1.75, backupVal.doubleValue(), 0.0001);
    }

    // =========================================================================
    // SCALARS: MSET / EXPIRE / GETDEL
    // =========================================================================

    @Test
    @DisplayName("Replication: MSET via Redis → BACKUP via gRPC")
    void testMSetReplication() throws Exception {
        String key1 = uniqueKeyWithTag("repl_mset1");
        String key2 = uniqueKeyWithTag("repl_mset2");

        jedis.mset(key1, "mv1", key2, "mv2");
        awaitReplication();

        smart().setMode(Mode.MASTER);
        assertEqualsVariantString(client.get(key1).get(), "mv1");
        assertEqualsVariantString(smart().setMode(Mode.BACKUP).get(key1).get(), "mv1");
        assertEqualsVariantString(smart().setMode(Mode.BACKUP).get(key2).get(), "mv2");
    }

    @Test
    @DisplayName("Replication: EXPIRE via Redis → TTL on BACKUP via gRPC")
    void testExpireReplication() throws Exception {
        String key = uniqueKeyWithTag("repl_expire");

        jedis.set(key, "expiring");
        jedis.expire(key, 600);
        awaitReplication();

        smart().setMode(Mode.MASTER);
        Long masterTtl = client.getTtl(key).get();
        assertNotNull(masterTtl);
        assertTrue(masterTtl > 0);

        Long backupTtl = smart().setMode(Mode.BACKUP).getTtl(key).get();
        assertNotNull(backupTtl);
        assertTrue(backupTtl > 0);
    }

    @Test
    @DisplayName("Replication: GETDEL via Redis → absent on BACKUP")
    void testGetDelReplication() throws Exception {
        String key = uniqueKeyWithTag("repl_getdel");

        jedis.set(key, "to_delete");
        assertEquals("to_delete", jedis.getDel(key));
        awaitReplication();

        smart().setMode(Mode.MASTER);
        assertFalse(client.containsKey(key).get());
        assertFalse(smart().setMode(Mode.BACKUP).containsKey(key).get());
    }
}