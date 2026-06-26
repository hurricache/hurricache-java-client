package com.hurricache.redis;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.LongAdder;

import static org.junit.jupiter.api.Assertions.fail;

public class RedisStressTest {

    private static JedisPool jedisPool;
    private static final String REDIS_HOST = "localhost";
    private static final int REDIS_PORT = 6379;

    private static final int THREAD_COUNT = 32;
    private static final int KEYS_PER_THREAD = 100_000;
    private static final int PIPELINE_BATCH_SIZE = 1000; // Размер пайплайна

    private final LongAdder totalCreateTimeNs = new LongAdder();
    private final LongAdder totalReadTimeNs = new LongAdder();
    private final LongAdder totalDeleteTimeNs = new LongAdder();

    @BeforeAll
    static void setUp() {
        JedisPoolConfig poolConfig = new JedisPoolConfig();
        poolConfig.setMaxTotal(THREAD_COUNT + 10);
        poolConfig.setMaxIdle(THREAD_COUNT);
        poolConfig.setMinIdle(10);

        jedisPool = new JedisPool(poolConfig, REDIS_HOST, REDIS_PORT);

        try (Jedis jedis = jedisPool.getResource()) {
            System.out.println("Connected to Redis. Ping: " + jedis.ping());
        } catch (Exception e) {
            fail("Redis не отвечает: " + e.getMessage());
        }
    }

    @AfterAll
    static void tearDown() {
        if (jedisPool != null) {
            jedisPool.close();
        }
    }

    @Test
    void testRedisThroughputAndLatency() throws InterruptedException {
        ExecutorService executor = Executors.newFixedThreadPool(THREAD_COUNT);
        List<Callable<Void>> tasks = new ArrayList<>();

        System.out.println("Запуск стресс-теста: " + THREAD_COUNT + " потоков по " + KEYS_PER_THREAD + " ключей каждый...");
        long totalTestStart = System.currentTimeMillis();

        // Формируем задачи для каждого потока
        for (int threadId = 0; threadId < THREAD_COUNT; threadId++) {
            final int finalThreadId = threadId;
            tasks.add(() -> {
                // Каждый поток берет свое выделенное соединение из пула
                try (Jedis jedis = jedisPool.getResource()) {

                    // --- ЭТАП 1: СОЗДАНИЕ (CREATE) ---
                    long startCreate = System.nanoTime();
                    for (int i = 0; i < KEYS_PER_THREAD; i++) {
                        String key = "stress:t" + finalThreadId + ":k" + i;
                        jedis.set(key, "value_" + i);
                    }
                    totalCreateTimeNs.add(System.nanoTime() - startCreate);

                    // --- ЭТАП 2: ЧТЕНИЕ (READ) ---
                    long startRead = System.nanoTime();
                    for (int i = 0; i < KEYS_PER_THREAD; i++) {
                        String key = "stress:t" + finalThreadId + ":k" + i;
                        String val = jedis.get(key);
                    }
                    totalReadTimeNs.add(System.nanoTime() - startRead);

                    // --- ЭТАП 3: УДАЛЕНИЕ (DELETE) ---
                    long startDelete = System.nanoTime();
                    for (int i = 0; i < KEYS_PER_THREAD; i++) {
                        String key = "stress:t" + finalThreadId + ":k" + i;
                        jedis.del(key);
                    }
                    totalDeleteTimeNs.add(System.nanoTime() - startDelete);

                } catch (Exception e) {
                    System.err.println("Ошибка в потоке " + finalThreadId + ": " + e.getMessage());
                }
                return null;
            });
        }

        // Запускаем все 32 потока одновременно и ждем завершения
        List<Future<Void>> futures = executor.invokeAll(tasks);
        for (Future<Void> future : futures) {
            try {
                future.get(); // Проверяем, не вылетело ли исключений
            } catch (Exception e) {
                fail("Один из потоков завершился с ошибкой: " + e.getMessage());
            }
        }

        executor.shutdown();

        long totalTestTimeMs = System.currentTimeMillis() - totalTestStart;
        long totalOperations = (long) THREAD_COUNT * KEYS_PER_THREAD;

        // Вывод результатов
        printResults("CREATE", totalCreateTimeNs.sum(), totalOperations);
        printResults("READ", totalReadTimeNs.sum(), totalOperations);
        printResults("DELETE", totalDeleteTimeNs.sum(), totalOperations);

        System.out.println("=================================================");
        System.out.printf("Полное время выполнения теста: %,d мс\n", totalTestTimeMs);
        System.out.printf("Общее количество успешно обработанных операций: %,d\n", totalOperations * 3);
        System.out.printf("Суммарный RPS (операций в секунду): %,d\n", (totalOperations * 3 * 1000) / totalTestTimeMs);
    }

    @Test
    void testRedisPipeliningThroughput() throws InterruptedException {
        ExecutorService executor = Executors.newFixedThreadPool(THREAD_COUNT);
        List<Callable<Void>> tasks = new ArrayList<>();

        System.out.println("Запуск PIPELINE стресс-теста: " + THREAD_COUNT + " потоков.");
        System.out.println("Каждый поток отправляет " + KEYS_PER_THREAD + " ключей пачками по " + PIPELINE_BATCH_SIZE + "...");
        long totalTestStart = System.currentTimeMillis();

        for (int threadId = 0; threadId < THREAD_COUNT; threadId++) {
            final int finalThreadId = threadId;
            tasks.add(() -> {
                try (Jedis jedis = jedisPool.getResource()) {

                    // --- ЭТАП 1: СОЗДАНИЕ (CREATE) С ПАЙПЛАЙНОМ ---
                    long startCreate = System.nanoTime();
                    for (int i = 0; i < KEYS_PER_THREAD; i += PIPELINE_BATCH_SIZE) {
                        Pipeline pipeline = jedis.pipelined();
                        for (int j = 0; j < PIPELINE_BATCH_SIZE && (i + j) < KEYS_PER_THREAD; j++) {
                            String key = "stress:t" + finalThreadId + ":k" + (i + j);
                            pipeline.set(key, "value_" + (i + j));
                        }
                        // Сбрасываем буфер в сеть и ждем ответов всей пачки за раз
                        pipeline.sync();
                    }
                    totalCreateTimeNs.add(System.nanoTime() - startCreate);

                    // --- ЭТАП 2: ЧТЕНИЕ (READ) С ПАЙПЛАЙНОМ ---
                    long startRead = System.nanoTime();
                    for (int i = 0; i < KEYS_PER_THREAD; i += PIPELINE_BATCH_SIZE) {
                        Pipeline pipeline = jedis.pipelined();
                        for (int j = 0; j < PIPELINE_BATCH_SIZE && (i + j) < KEYS_PER_THREAD; j++) {
                            String key = "stress:t" + finalThreadId + ":k" + (i + j);
                            pipeline.get(key);
                        }
                        // syncAndReturnAll вернет List<Object> с результатами get(),
                        // но для замера скорости вычитывания сырых данных нам сам List не важен.
                        pipeline.sync();
                    }
                    totalReadTimeNs.add(System.nanoTime() - startRead);

                    // --- ЭТАП 3: УДАЛЕНИЕ (DELETE) С ПАЙПЛАЙНОМ ---
                    long startDelete = System.nanoTime();
                    for (int i = 0; i < KEYS_PER_THREAD; i += PIPELINE_BATCH_SIZE) {
                        Pipeline pipeline = jedis.pipelined();
                        for (int j = 0; j < PIPELINE_BATCH_SIZE && (i + j) < KEYS_PER_THREAD; j++) {
                            String key = "stress:t" + finalThreadId + ":k" + (i + j);
                            pipeline.del(key);
                        }
                        pipeline.sync();
                    }
                    totalDeleteTimeNs.add(System.nanoTime() - startDelete);

                } catch (Exception e) {
                    System.err.println("Ошибка в потоке " + finalThreadId + ": " + e.getMessage());
                }
                return null;
            });
        }

        List<Future<Void>> futures = executor.invokeAll(tasks);
        for (Future<Void> future : futures) {
            try {
                future.get();
            } catch (Exception e) {
                fail("Поток завершился падением: " + e.getMessage());
            }
        }

        executor.shutdown();

        long totalTestTimeMs = System.currentTimeMillis() - totalTestStart;
        long totalOperations = (long) THREAD_COUNT * KEYS_PER_THREAD;

        printResults("CREATE (Pipeline 100)", totalCreateTimeNs.sum(), totalOperations);
        printResults("READ (Pipeline 100)", totalReadTimeNs.sum(), totalOperations);
        printResults("DELETE (Pipeline 100)", totalDeleteTimeNs.sum(), totalOperations);

        System.out.println("=================================================");
        System.out.printf("Test duration: %,d ms\n", totalTestTimeMs);
        System.out.printf("Total operations: %,d\n", totalOperations * 3);
        System.out.printf("Overall RPS: %,d rps\n", (totalOperations * 3 * 1000) / totalTestTimeMs);
    }

    private void printResults(String opName, long totalTimeNs, long opCount) {
        double totalTimeMs = totalTimeNs / 1_000_000.0;
        double avgLatencyNs = (double) totalTimeNs / opCount;
        long rps = (long) (opCount / (totalTimeMs / 1000.0));

        System.out.println("-------------------------------------------------");
        System.out.printf("Operation: [%s]\n", opName);
        System.out.printf("  Total threads execution time (combined): %.2f ms\n", totalTimeMs);
        System.out.printf("  Amortized latency per operation: %.2f μs\n", avgLatencyNs / 1000.0);
        System.out.printf("  Effective RPS: %,d\n", rps);
    }
}