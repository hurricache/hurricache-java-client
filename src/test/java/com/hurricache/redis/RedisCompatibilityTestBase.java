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
package com.hurricache.redis;

import com.hurricache.client.intf.HurriCacheClientInterface;
import com.hurricache.client.intf.Variant;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import redis.clients.jedis.DefaultJedisClientConfig;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisClientConfig;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.commands.DatabaseCommands;
import redis.clients.jedis.commands.JedisBinaryCommands;
import redis.clients.jedis.commands.JedisCommands;
import redis.clients.jedis.commands.ServerCommands;
import redis.clients.jedis.exceptions.JedisMovedDataException;

import java.io.Closeable;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Abstract base class for Redis compatibility tests.
 * Provides bidirectional testing between Redis protocol (via Jedis) and HurriCache gRPC client.
 *
 * Осознанно НЕ поддерживаются коннектором (тестировать не нужно):
 * SINTER/SUNION/SDIFF (возвращают пустой массив), HRANDFIELD, RANDOMKEY (nil),
 * RENAME/RENAMENX (not supported), pub/sub, MULTI/EXEC/WATCH, EVAL.
 */
public abstract class RedisCompatibilityTestBase {

    private DefaultJedisClientConfig clientConfig;

    /**
     * Набор команд Jedis, используемый тестами (объединение интерфейсов Jedis).
     * Нужен для проксирования с обработкой MOVED.
     */
    public interface RedisTestJedis extends JedisCommands, JedisBinaryCommands, DatabaseCommands, ServerCommands, Closeable {
    }

    protected RedisTestJedis jedis;
    protected JedisPool jedisPool;
    protected HurriCacheClientInterface client;

    // On-demand клиенты на адреса из MOVED-ответов (кластерный режим)
    private final Map<HostAndPort, Jedis> movedClients = new ConcurrentHashMap<>();

    /**
     * Returns the HurriCache client implementation to test.
     * Must be implemented by subclasses for standalone/cluster modes.
     */
    protected abstract HurriCacheClientInterface getClient();

    /**
     * Returns the server name for test reporting.
     */
    protected abstract String getServerName();

    /**
     * Returns the timeout for gRPC operations.
     */
    protected Duration getGrpcTimeout() {
        return Duration.ofSeconds(30);
    }

    /**
     * Returns the TTL for created containers.
     */
    protected Duration getContainerTtl() {
        return Duration.ofMinutes(5);
    }

    /**
     * Ожидание replication lag после мутирующих операций.
     * Вызывать перед чтениями через jedis (RESP): RESP-нода может оказаться бэкапом
     * для ключа, и репликация с мастера приходит асинхронно.
     * Чтения через gRPC smart-клиент ждать не требуют — они роутятся на мастер.
     */
    protected void awaitReplication() {
        try {
            Thread.sleep(getReplicationLagMs());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    protected long getReplicationLagMs() {
        return Long.getLong("hurricache.test.replicationLagMs", 500L);
    }

    @BeforeEach
    void setUp() {
        // Setup Jedis connection to Redis
        JedisPoolConfig poolConfig = new JedisPoolConfig();
        poolConfig.setMaxTotal(10);
        poolConfig.setMaxIdle(5);
        poolConfig.setMinIdle(2);
        clientConfig = DefaultJedisClientConfig.builder()
                .connectionTimeoutMillis(15000) // Таймаут на установку TCP-соединения
                .socketTimeoutMillis(15000)     // Таймаут на чтение (ожидание ответа от сервера)
                .build();
        HostAndPort hostAndPort = new HostAndPort("localhost", 6379);
        jedisPool = new JedisPool(poolConfig, hostAndPort,clientConfig);
        jedis = wrapWithMovedRetry(jedisPool.getResource());

        // Initialize HurriCache client
        client = getClient();

        // Clean up before each test
//        try {
//            jedis.flushDB();
//        } catch (Exception e) {
//            System.err.println("Warning: Could not flush Redis: " + e.getMessage());
//        }
    }

    @AfterEach
    void tearDown() {
        try {
            if (jedis != null) {
                jedis.close();
            }
        } catch (Exception e) {
            // Ignore
        }

        for (Jedis mc : movedClients.values()) {
            try {
                mc.close();
            } catch (Exception e) {
                // Ignore
            }
        }
        movedClients.clear();

        try {
            if (jedisPool != null) {
                jedisPool.close();
            }
        } catch (Exception e) {
            // Ignore
        }

        try {
            if (client != null) {
                client.shutdown();
            }
        } catch (Exception e) {
            // Ignore
        }
    }

    /**
     * Оборачивает Jedis проксом: при JedisMovedDataException (ответ MOVED от кластера)
     * вызов повторяется клиентом на адрес из MOVED (создаётся on-demand).
     * Для standalone прокси прозрачен (MOVED не приходит).
     */
    private RedisTestJedis wrapWithMovedRetry(Jedis real) {
        InvocationHandler handler = (proxy, method, args) -> {
            if (method.getDeclaringClass() == Object.class) {
                return method.invoke(real, args);
            }
            try {
                return method.invoke(real, args);
            } catch (InvocationTargetException e) {
                Throwable cause = e.getCause();
                if (cause instanceof JedisMovedDataException moved) {
                    Jedis target = jedisFor(moved.getTargetNode());
                    try {
                        return method.invoke(target, args);
                    } catch (InvocationTargetException e2) {
                        throw e2.getCause();
                    }
                }
                throw cause;
            }
        };
        return (RedisTestJedis) Proxy.newProxyInstance(getClass().getClassLoader(),
                                                       new Class<?>[]{RedisTestJedis.class}, handler);
    }

    private Jedis jedisFor(HostAndPort target) {
        return movedClients.computeIfAbsent(target, t -> new Jedis(t.getHost(), t.getPort(),clientConfig));
    }

    /**
     * Helper to compare Variant bytes with expected string.
     */
    protected void assertEqualsVariantString(Variant variant, String expected) {
        if (variant == null) {
            org.junit.jupiter.api.Assertions.fail("Variant is null, expected: " + expected);
        }
        String actual = new String(variant.bytesValue(), StandardCharsets.UTF_8);
        org.junit.jupiter.api.Assertions.assertEquals(expected, actual,
            "Mismatch for server: " + getServerName());
    }

    /**
     * Helper to compare Variant numbers with expected value.
     */
    protected void assertEqualsVariantLong(Variant variant, long expected) {
        if (variant == null) {
            org.junit.jupiter.api.Assertions.fail("Variant is null, expected: " + expected);
        }
        long actual = variant.longValue();
        org.junit.jupiter.api.Assertions.assertEquals(expected, actual,
            "Mismatch for server: " + getServerName());
    }

    /**
     * Helper to create a unique key prefix.
     */
    protected String uniqueKey(String prefix) {
        String s = prefix + ":" + UUID.randomUUID().toString().substring(0, 8);
        System.out.println("uniqueKey: " + s );
        return s;
    }

    /**
     * Helper to create a unique key with cluster tag.
     */
    protected String uniqueKeyWithTag(String prefix) {
        String s = prefix + ":{" + UUID.randomUUID().toString().substring(0, 8) + "}";
        System.out.println("uniqueKeyWithTag: " + s);
        return s;
    }

    /**
     * Helper to create Variant from string.
     */
    protected Variant toVariant(String value) {
        return Variant.ofBytes(value.getBytes(StandardCharsets.UTF_8));
    }

    /**
     * Helper to create Variant from long.
     */
    protected Variant toVariant(long value) {
        return Variant.ofLong(value);
    }

    /**
     * Helper to create Variant from double.
     */
    protected Variant toVariant(double value) {
        return Variant.ofDouble(value);
    }
}