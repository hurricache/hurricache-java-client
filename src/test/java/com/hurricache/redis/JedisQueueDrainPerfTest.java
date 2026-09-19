package com.hurricache.redis;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.Pipeline;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.LongAdder;

public class JedisQueueDrainPerfTest {

    private static final int MESSAGE_SIZE = 100;
    private static final int READER_THREADS = 16;
    private static final int DURATION_PER_STAGE_SECONDS = 5;
    private static final int PIPELINE_BATCH_SIZE = 100; // Аналог in-flight окна

    private static final int[] DEFAULT_SIZES = {16, 64, 512, 4096, 32768, 262144};
    private static JedisPool jedisPool;

    @BeforeAll
    public static void setup() {
        JedisPoolConfig poolConfig = new JedisPoolConfig();
        poolConfig.setMaxTotal(READER_THREADS + 2);
        poolConfig.setMaxIdle(READER_THREADS + 2);

        // Подключение к Redis (Standalone или Master-нода кластера)
        jedisPool = new JedisPool(poolConfig, "127.0.0.1", 6379, 5000);
    }

    @AfterAll
    public static void teardown() {
        if (jedisPool != null) {
            jedisPool.close();
        }
    }

    private int[] parseTargetSizes() {
        String sizesProp = System.getProperty("queue.sizes");
        if (sizesProp == null || sizesProp.trim().isEmpty()) return DEFAULT_SIZES;
        return Arrays.stream(sizesProp.split(",")).map(String::trim).mapToInt(Integer::parseInt).toArray();
    }

    @Test
    void benchmarkJedisQueueDrain() throws Exception {
        int[] targetSizes = parseTargetSizes();

        System.out.println("===============================================================");
        System.out.println(" STARTING JEDIS PIPELINED QUEUE DRAIN (LPOP) BENCHMARK ");
        System.out.println(" Target sizes to test: " + Arrays.toString(targetSizes));
        System.out.println(" Threads: " + READER_THREADS + " | Pipeline Batch Window: " + PIPELINE_BATCH_SIZE);
        System.out.println("===============================================================");

        for (int size : targetSizes) {
            runBenchmarkForSize(size);
        }
    }

    private void runBenchmarkForSize(int queueSize) throws Exception {
        byte[] queueKey = ("perf-jedis-queue-" + queueSize + "-" + System.currentTimeMillis()).getBytes();

        // 1. Подготовка и быстрая заливка данных через Pipeline
        try (Jedis jedis = jedisPool.getResource()) {
            Pipeline p = jedis.pipelined();
            for (int i = 0; i < queueSize; i++) {
                p.rpush(queueKey, generate100ByteString("val-" + i));
                if (i % 5000 == 0) p.sync(); // Сбрасываем чанками, чтобы буфер не переполнился
            }
            p.expire(queueKey, Duration.ofMinutes(15).toSeconds());
            p.sync();
        }

        // 2. Настройка потоков-читателей
        ExecutorService pool = Executors.newFixedThreadPool(READER_THREADS);
        LongAdder readOps = new LongAdder();
        LongAdder readErrors = new LongAdder();

        AtomicBoolean running = new AtomicBoolean(true);
        AtomicBoolean queueIsEmpty = new AtomicBoolean(false);
        CountDownLatch startLatch = new CountDownLatch(1);

        for (int i = 0; i < READER_THREADS; i++) {
            pool.submit(() -> {
                try {
                    startLatch.await();

                    try (Jedis jedis = jedisPool.getResource()) {
                        while (running.get() && !queueIsEmpty.get()) {
                            Pipeline pipeline = jedis.pipelined();

                            // Забиваем конвейер пачкой LPOP
                            for (int b = 0; b < PIPELINE_BATCH_SIZE; b++) {
                                pipeline.lpop(queueKey);
                            }

                            // Выполняем пачку за один сетевой round-trip
                            List<Object> responses = pipeline.syncAndReturnAll();

                            int validReads = 0;
                            for (Object res : responses) {
                                if (res instanceof byte[]) {
                                    byte[] data = (byte[]) res;
                                    if (data.length == 0) {
                                        queueIsEmpty.set(true);
                                        break;
                                    }
                                    validReads++;
                                } else if (res == null) {
                                    // Очередь пуста
                                    queueIsEmpty.set(true);
                                    break;
                                } else if (res instanceof Exception) {
                                    readErrors.increment();
                                }
                            }

                            readOps.add(validReads);
                        }
                    }
                } catch (Exception e) {
                    readErrors.increment();
                }
            });
        }

        long startTime = System.nanoTime();
        startLatch.countDown();

        long maxWaitTimeMs = DURATION_PER_STAGE_SECONDS * 1000L;
        long checkIntervalMs = 50;
        long spentTimeMs = 0;

        while (spentTimeMs < maxWaitTimeMs && !queueIsEmpty.get()) {
            Thread.sleep(checkIntervalMs);
            spentTimeMs += checkIntervalMs;
        }

        running.set(false);
        pool.shutdown();
        pool.awaitTermination(5, TimeUnit.SECONDS);

        long elapsedTimeNs = System.nanoTime() - startTime;
        double elapsedTimeSec = elapsedTimeNs / 1_000_000_000.0;
        long totalReads = readOps.sum();
        double tps = totalReads / elapsedTimeSec;

        String terminationReason = queueIsEmpty.get() ? "QUEUE EMPTIED" : "TIME OUT";

        System.out.printf("Jedis Queue Size: %-7d | Drained: %-7d | Drained TPS: %-10.2f | Errors: %-3d | Status: %s%n",
                          queueSize, totalReads, tps, readErrors.sum(), terminationReason);

        // Чистим ключ
        try (Jedis jedis = jedisPool.getResource()) {
            jedis.del(queueKey);
        }
    }

    private byte[] generate100ByteString(String prefix) {
        byte[] prefixBytes = prefix.getBytes();
        byte[] result = new byte[MESSAGE_SIZE];
        System.arraycopy(prefixBytes, 0, result, 0, Math.min(prefixBytes.length, MESSAGE_SIZE));
        for (int i = prefixBytes.length; i < MESSAGE_SIZE; i++) {
            result[i] = 'x';
        }
        return result;
    }
}