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

public class JedisVectorIndexNoPipelinePerfTest {

    private static final int MESSAGE_SIZE = 100;
    private static final int READER_THREADS = 16;
    private static final int DURATION_PER_STAGE_SECONDS = 5;

    private static final int[] DEFAULT_SIZES = {16, 64, 512, 4096, 32768, 262144};
    private static JedisPool jedisPool;

    @BeforeAll
    public static void setup() {
        JedisPoolConfig poolConfig = new JedisPoolConfig();
        poolConfig.setMaxTotal(READER_THREADS + 2);
        poolConfig.setMaxIdle(READER_THREADS + 2);
        jedisPool = new JedisPool(poolConfig, "127.0.0.1", 6379, 5000);
    }

    @AfterAll
    public static void teardown() {
        if (jedisPool != null) {
            jedisPool.close();
        }
    }

    private int[] parseTargetSizes() {
        String sizesProp = System.getProperty("vector.sizes");
        if (sizesProp == null || sizesProp.trim().isEmpty()) return DEFAULT_SIZES;
        return Arrays.stream(sizesProp.split(",")).map(String::trim).mapToInt(Integer::parseInt).toArray();
    }

    @Test
    void benchmarkJedisVectorAccessNoPipeline() throws Exception {
        int[] targetSizes = parseTargetSizes();

        System.out.println("===============================================================");
        System.out.println(" STARTING JEDIS STANDARD (NO PIPELINE) VECTOR ACCESS (LINDEX) ");
        System.out.println(" Target sizes to test: " + Arrays.toString(targetSizes));
        System.out.println(" Threads: " + READER_THREADS);
        System.out.println("===============================================================");

        for (int size : targetSizes) {
            runBenchmarkForSize(size);
        }
    }

    private void runBenchmarkForSize(int vectorSize) throws Exception {
        byte[] vectorKey = ("perf-jedis-np-vector-" + vectorSize + "-" + System.currentTimeMillis()).getBytes();

        // 1. Создание вектора
        try (Jedis jedis = jedisPool.getResource()) {
            for (int i = 0; i < vectorSize; i++) {
                jedis.rpush(vectorKey, generate100ByteString("val-" + i));
            }
            jedis.expire(vectorKey, Duration.ofMinutes(15).toSeconds());
        }

        // 2. Потоки для случайного чтения по индексу синхронно
        ExecutorService pool = Executors.newFixedThreadPool(READER_THREADS);
        LongAdder readOps = new LongAdder();
        LongAdder readErrors = new LongAdder();

        AtomicBoolean running = new AtomicBoolean(true);
        CountDownLatch startLatch = new CountDownLatch(1);

        for (int i = 0; i < READER_THREADS; i++) {
            pool.submit(() -> {
                try {
                    startLatch.await();
                    ThreadLocalRandom random = ThreadLocalRandom.current();

                    try (Jedis jedis = jedisPool.getResource()) {
                        while (running.get()) {
                            try {
                                int randomIdx = random.nextInt(vectorSize);
                                byte[] res = jedis.lindex(vectorKey, randomIdx);

                                if (res != null && res.length > 0) {
                                    readOps.increment();
                                }
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

        // Прогрев
        startLatch.countDown();
        Thread.sleep(500);

        readOps.reset();
        readErrors.reset();
        long startTime = System.nanoTime();

        Thread.sleep(DURATION_PER_STAGE_SECONDS * 1000L);
        running.set(false);

        pool.shutdown();
        pool.awaitTermination(5, TimeUnit.SECONDS);

        long elapsedTimeNs = System.nanoTime() - startTime;
        double elapsedTimeSec = elapsedTimeNs / 1_000_000_000.0;
        long totalReads = readOps.sum();
        double tps = totalReads / elapsedTimeSec;

        System.out.printf("Jedis Vector Size: %-7d | Elements | Read TPS: %-10.2f | Errors: %d%n",
                          vectorSize, tps, readErrors.sum());

        // Чистка ключа
        try (Jedis jedis = jedisPool.getResource()) {
            jedis.del(vectorKey);
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