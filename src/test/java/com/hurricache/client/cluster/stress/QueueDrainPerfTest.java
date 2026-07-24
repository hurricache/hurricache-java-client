package com.hurricache.client.cluster.stress;

import com.hurricache.client.FastCacheAsyncSmartClient;
import com.hurricache.grpc.KeyHint;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.LongAdder;

import static org.junit.jupiter.api.Assertions.assertNotNull;

public class QueueDrainPerfTest {

    private static final int MESSAGE_SIZE = 100; // байт
    private static final Duration TIMEOUT = Duration.ofSeconds(5);

    // Настройки параллелизма для чтения (выгребания)
    private static final int READER_THREADS = 16;
    private static final int DURATION_PER_STAGE_SECONDS = 5;
    private static final int MAX_IN_FLIGHT_PER_THREAD = 100;

    // НАСТРОЙКА РАЗМЕРОВ: дефолтные значения количества элементов в очереди
    private static final int[] DEFAULT_SIZES = {16, 64, 512, 4096, 32768, 262144};

    private static FastCacheAsyncSmartClient client;

    @BeforeAll
    public static void setup() throws Exception {
        client = new FastCacheAsyncSmartClient("127.0.0.1", 51000, 0, TIMEOUT) {
            public Duration getDefaultTtl() {
                return Duration.ofMinutes(15);
            }
        };
    }

    @AfterAll
    public static void teardown() throws Exception {
        if (client != null) {
            client.shutdown();
        }
    }

    /**
     * Парсит размеры очередей из системного свойства "queue.sizes".
     * Если свойство не задано, возвращает дефолтный набор.
     */
    private int[] parseTargetSizes() {
        String sizesProp = System.getProperty("queue.sizes");
        if (sizesProp == null || sizesProp.trim().isEmpty()) {
            return DEFAULT_SIZES;
        }
        try {
            return Arrays.stream(sizesProp.split(","))
                    .map(String::trim)
                    .mapToInt(Integer::parseInt)
                    .toArray();
        } catch (NumberFormatException e) {
            System.err.println("[WARN] Invalid format for queue.sizes property. Falling back to defaults.");
            return DEFAULT_SIZES;
        }
    }

    @Test
    void benchmarkQueueDrainAtDifferentSizes() throws Exception {
        int[] targetSizes = parseTargetSizes();

        System.out.println("===============================================================");
        System.out.println(" STARTING QUEUE DRAIN (GET AND REMOVE FRONT) BENCHMARK ");
        System.out.println(" Target sizes to test: " + Arrays.toString(targetSizes));
        System.out.println(" Threads: " + READER_THREADS + " | In-flight window per thread: " + MAX_IN_FLIGHT_PER_THREAD);
        System.out.println("===============================================================");

        for (int size : targetSizes) {
            runBenchmarkForSize(size);
        }
    }

    private void runBenchmarkForSize(int queueSize) throws Exception {
        String queueKeyStr = "perf-queue-" + queueSize + "-" + System.currentTimeMillis();
        byte[] queueKey = queueKeyStr.getBytes(java.nio.charset.StandardCharsets.UTF_8);

        // 1. Подготовка данных нужного размера
        List<byte[]> initialData = new ArrayList<>(queueSize);
        for (int i = 0; i < queueSize; i++) {
            initialData.add(generate100ByteString("val-" + i));
        }

        // 2. Создание очереди на сервере с автоматическим рекурсивным чанкованием хвоста
        KeyHint queueHint = client.createQueue(
                queueKey,
                initialData,
                Duration.ofMinutes(15),
                0,
                TIMEOUT
        ).get();

        assertNotNull(queueHint, "Queue must be created successfully for size: " + queueSize);

        // 3. Запуск потоков чтения (выгребания)
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
                    Semaphore inFlightWindow = new Semaphore(MAX_IN_FLIGHT_PER_THREAD);

                    // Выгребаем пока работает таймер бенчмарка И в очереди еще что-то есть
                    while (running.get() && !queueIsEmpty.get()) {
                        inFlightWindow.acquire();

                        client.getAndRemoveFront(queueKey, queueHint, 0, TIMEOUT)
                                .whenComplete((res, ex) -> {
                                    inFlightWindow.release();
                                    if (ex == null) {
                                        // Если сервер вернул пустоту — значит очередь полностью выгребли
                                        if (res == null || res.length == 0) {
                                            queueIsEmpty.set(true);
                                        } else {
                                            readOps.increment();
                                        }
                                    } else {
                                        readErrors.increment();
                                    }
                                });
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
        }

        // Старт! (Прогрев убран, так как элементы удаляются физически, меряем чистую скорость опустошения)
        long startTime = System.nanoTime();
        startLatch.countDown();

        // Ждем окончания времени теста или полного опустошения очереди
        long maxWaitTimeMs = DURATION_PER_STAGE_SECONDS * 1000L;
        long checkIntervalMs = 50;
        long spentTimeMs = 0;

        while (spentTimeMs < maxWaitTimeMs && !queueIsEmpty.get()) {
            Thread.sleep(checkIntervalMs);
            spentTimeMs += checkIntervalMs;
        }

        // Останавливаем сбор данных
        running.set(false);

        pool.shutdown();
        pool.awaitTermination(5, TimeUnit.SECONDS);

        long elapsedTimeNs = System.nanoTime() - startTime;
        double elapsedTimeSec = elapsedTimeNs / 1_000_000_000.0;
        long totalReads = readOps.sum();
        double tps = totalReads / elapsedTimeSec;

        String terminationReason = queueIsEmpty.get() ? "QUEUE EMPTIED" : "TIME OUT";

        System.out.printf("Queue Size: %-7d | Drained: %-7d | Drained TPS: %-10.2f | Errors: %-3d | Status: %s%n",
                          queueSize, totalReads, tps, readErrors.sum(), terminationReason);
    }

    private byte[] generate100ByteString(String prefix) {
        byte[] prefixBytes = prefix.getBytes(java.nio.charset.StandardCharsets.UTF_8);
        byte[] result = new byte[MESSAGE_SIZE];
        System.arraycopy(prefixBytes, 0, result, 0, Math.min(prefixBytes.length, MESSAGE_SIZE));
        for (int i = prefixBytes.length; i < MESSAGE_SIZE; i++) {
            result[i] = 'x';
        }
        return result;
    }
}