package com.hurricache.redis;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.time.Duration;
import java.util.Arrays;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.LongAdder;

public class JedisQueueDrainNoPipelinePerfTest {

    private static final int MESSAGE_SIZE = 100;
    private static final int READER_THREADS = 16;
    private static final int DURATION_PER_STAGE_SECONDS = 5;

    private static final int[] DEFAULT_SIZES = {16, 64, 512, 4096, 32768, 262144};
    private static JedisPool jedisPool;

    @BeforeAll
    public static void setup() {
        JedisPoolConfig poolConfig = new JedisPoolConfig();
        // Пул должен быть равен числу потоков или чуть больше, чтобы избежать блокировок пула
        poolConfig.setMaxTotal(READER_THREADS + 2);
        poolConfig.setMaxIdle(READER_THREADS + 2);

        // Подключение к Redis
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
    void benchmarkJedisQueueDrainNoPipeline() throws Exception {
        int[] targetSizes = parseTargetSizes();

        System.out.println("===============================================================");
        System.out.println(" STARTING JEDIS STANDARD (NO PIPELINE) QUEUE DRAIN (LPOP) ");
        System.out.println(" Target sizes to test: " + Arrays.toString(targetSizes));
        System.out.println(" Threads: " + READER_THREADS);
        System.out.println("===============================================================");

        for (int size : targetSizes) {
            runBenchmarkForSize(size);
        }
    }

    private void runBenchmarkForSize(int queueSize) throws Exception {
        byte[] queueKey = ("perf-jedis-np-queue-" + queueSize + "-" + System.currentTimeMillis()).getBytes();

        // 1. Создание очереди: заливка данных поэлементно (синхронно)
        try (Jedis jedis = jedisPool.getResource()) {
            for (int i = 0; i < queueSize; i++) {
                jedis.rpush(queueKey, generate100ByteString("val-" + i));
            }
            jedis.expire(queueKey, Duration.ofMinutes(15).toSeconds());
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
                        // Каждый поток блокирующе читает по одному элементу на итерацию
                        while (running.get() && !queueIsEmpty.get()) {
                            try {
                                byte[] res = jedis.lpop(queueKey);

                                if (res == null || res.length == 0) {
                                    // Сигнал, что элементы закончились
                                    queueIsEmpty.set(true);
                                    break;
                                }
                                readOps.increment();
                            } catch (Exception e) {
                                readErrors.increment();
                            }
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

        // Ждем опустошения или таймаута
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

        // Чистка ключа
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